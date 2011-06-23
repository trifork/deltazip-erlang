-module(deltazip).
-compile(export_all).

-export([open/1, get/1, previous/1, add/2, add_multiple/2, close/1]).
-export([stats_for_current_entry/1]).

-record(dzstate, {get_size_fun :: fun(() -> integer()),
		  pread_fun    :: fun((integer(),integer()) -> {ok, binary()} | {error,_}),
		  zip_handle,
		  file_header_state :: empty | valid,
		  current_pos :: integer(),
		  current_size :: integer(),
		  current_method :: integer(),
		  current_version :: binary() | file_is_empty
		 }).

-define(DELTAZIP_MAGIC_HEADER, <<16#CEB47A10:32/big>>). 
-define(FILE_HEADER_LENGTH, 4). 

%% Snapshot methods (0-3):
-define(METHOD_UNCOMPRESSED,   0).
-define(METHOD_DEFLATED,       1).
%% Delta methods (4-15):
-define(METHOD_CHUNKED,        4).
-define(METHOD_CHUNKED_MIDDLE, 5).
-define(METHOD_DITTOFLATE,     6). % Experimental

%% For some reason, zlib can only use 32K-262 bytes (=#7EFA) of a dictionary
%% when compressing.
%% (See http://www.zlib.net/manual.html)
%% So we use a slightly smaller window size:
-define(WINDOW_SIZE, 16#7E00).
-define(CHUNK_SIZE, (?WINDOW_SIZE div 2)). 

%% For when we need to ensure the deflated ouput will fit in 64KB:
%% (From http://www.zlib.net/zlib_tech.html:
%%  "...an overhead of five bytes per 16 KB block (about 0.03%), plus a one-time overhead of
%%   six bytes for the entire stream")
-define(LIMIT_SO_DEFLATED_FITS_IN_64KB, 65000).

-define(EXCLUDE_ZLIB_HEADERS, dummy). % Flag.

%%%-------------------- API --------------------

open(_Access={GetSizeFun, PReadFun}) when is_function(GetSizeFun,0),
				is_function(PReadFun,2) ->
    Z = zlib:open(),
    State = #dzstate{get_size_fun= GetSizeFun,
		     pread_fun   = PReadFun,
		     zip_handle  = Z},
    State2 = set_initial_position(check_magic_header(State)),
    case previous(State2) of
	{ok, State3} ->
	    State3;
	{error, at_beginning} ->
	    State2#dzstate{current_version = file_is_empty}
    end.

get(#dzstate{current_version=Bin}) ->
%%     io:format("DB| get @ ~p: ~p\n", [S#dzstate.current_pos, Bin]), 
    Bin.

stats_for_current_entry(#dzstate{current_method=M, current_size=Sz}) ->
    {M, Sz}.

-spec(previous/1 :: (#dzstate{}) -> {ok, #dzstate{}} | {error, at_beginning}).
previous(#dzstate{current_pos=CP}) when CP =< ?FILE_HEADER_LENGTH ->
    {error, at_beginning};
previous(State) ->
    {ok, compute_current_version(goto_previous_position(State))}.

add(State, NewRev) ->
    add_multiple(State, [NewRev]).

add_multiple(State, NewRevs) ->
    opt_prepend_file_header_to_append_spec(State, add_multiple2(State, NewRevs)).

add_multiple2(State, NewRevs) ->
    case previous(set_initial_position(State)) of
	{error, at_beginning} ->
	    Z = State#dzstate.zip_handle,
	    {0, pack_multiple(NewRevs, Z)};
	{ok, #dzstate{current_version = LastRev,
		      current_pos = PrefixLength,
		      zip_handle = Z}} ->
	    NewTail = pack_multiple([LastRev | NewRevs], Z),
	    {PrefixLength, NewTail}
    end.

close(#dzstate{zip_handle=Z}) ->
    zlib:close(Z).

%%%-------------------- Implementation --------------------

check_magic_header(State) ->
    case magic_header_state(State) of
	invalid     -> error(not_a_deltazip_file);
	HeaderState -> State#dzstate{file_header_state=HeaderState}
    end.

-spec(magic_header_state/1 :: (#dzstate{}) -> empty | invalid | valid).
magic_header_state(#dzstate{get_size_fun=GetSizeFun,
			    pread_fun=PReadFun}) ->
    Size = GetSizeFun(),
    if Size == 0 ->
	    empty;
       Size < 4 ->
	    invalid;
       Size >= 4 ->
	    case PReadFun(0,4) of
		{ok,Header} when Header == ?DELTAZIP_MAGIC_HEADER ->
		    valid;
		_ ->
		    invalid
	    end
    end.

set_initial_position(State=#dzstate{get_size_fun=GetSizeFun}) ->
    State#dzstate{current_pos=GetSizeFun()}.

goto_previous_position(State=#dzstate{current_pos=Pos}) ->
    <<Method:4, Size:28>>=Tag = safe_pread(State, Pos-4, 4),
    PrevPos = Pos - Size - 8,
    TagBefore = safe_pread(State, PrevPos, 4),
    if TagBefore /= Tag ->
	    error({envelope_error, [{end_position, Pos}, {end_tag,Tag},
				    {start_position, PrevPos}, {start_tag, TagBefore}]});
       true ->
	    State#dzstate{current_method=Method,
			  current_size=Size,
			  current_pos=PrevPos}
    end.

compute_current_version(State=#dzstate{current_pos=Pos,
				       current_size=Size,
				       current_method=Method}) ->
    Bin = safe_pread(State, Pos+4, Size),
    State#dzstate{current_version = unpack_entry(State, Method,Bin)}.

pack_multiple([], _Z) ->
    [];
pack_multiple([Data], Z) ->
    envelope(select_method_and_pack_snapshot(Data, Z));
pack_multiple([Data | [NextData|_]=Rest], Z) ->
    [envelope(select_method_and_pack_delta(Data, NextData, Z))
     | pack_multiple(Rest, Z)].

opt_prepend_file_header_to_append_spec(State, AppendSpec={Pos,Tail}) ->
    case State#dzstate.file_header_state of
	valid -> AppendSpec;
	empty when Pos == 0 -> {Pos, [?DELTAZIP_MAGIC_HEADER | Tail]}
    end.

%%%----------

safe_pread(#dzstate{pread_fun=PReadFun}, Pos, Size) ->
    case PReadFun(Pos, Size) of
	{error, Reason} ->
	    error({pread_error, Reason});
	{ok, Bin} ->
	    if byte_size(Bin) /= Size ->
		    error({pread_error, {returned_wrong_amount, Size, byte_size(Bin)}});
	       true ->
		    Bin
	    end
    end.    

envelope({Method, Data0}) ->
    Data = iolist_to_binary(Data0),
    Sz = byte_size(Data),
%%     io:format("DB| envelope: method=~p  size=~p\n", [Method, Sz]),
    Tag = <<Method:4, Sz:28>>,
    [Tag, Data, Tag].

%%%-------------------- Methods -------------------
unpack_entry(State, Method, Data) ->
%%     io:format("DB| unpack_entry: method=~p  raw-size=~p\n", [Method, byte_size(Data)]),
    iolist_to_binary(unpack_entry_to_iolist(State, Method, Data)).

unpack_entry_to_iolist(_State, ?METHOD_UNCOMPRESSED, Data) ->
    unpack_uncompressed(Data);
unpack_entry_to_iolist(State, ?METHOD_DEFLATED, Data) ->
    unpack_deflated(Data, State#dzstate.zip_handle);
unpack_entry_to_iolist(State, ?METHOD_CHUNKED, Data) ->
    unpack_chunked(Data, State#dzstate.current_version, State#dzstate.zip_handle);
unpack_entry_to_iolist(State, ?METHOD_CHUNKED_MIDDLE, Data) ->
    unpack_chunked_middle(Data, State#dzstate.current_version, State#dzstate.zip_handle).

select_method_and_pack_snapshot(Data, Z) ->
    pack_deflated(Data, Z).

select_method_and_pack_delta(Data, RefData, Z) ->
    AllowDitto = erlang:get(allow_dittoflate) /= undefined,
    ForceDitto = erlang:get(force_dittoflate) /= undefined,
    Methods = if ForceDitto ->
		      [fun pack_dittoflate/3];
		 true ->
		      [%fun pack_chunked/3,
		       fun pack_chunked_middle/3]
			  ++
			  [fun pack_dittoflate/3 || AllowDitto]
	      end,
    Packs = [begin P={_,Bin}=Method(Data, RefData, Z),
		   
		   {iolist_size(Bin), P}
	     end || Method <- Methods],
    {_,BestPack} = hd(lists:keysort(1,Packs)),
    BestPack.

%%%----- Method UNCOMPRESSED:

pack_uncompressed(Bin) -> {?METHOD_UNCOMPRESSED, Bin}.

unpack_uncompressed(Bin) -> Bin.

%%%----- Method DEFLATED:

pack_deflated(Bin, Z) ->
    {?METHOD_DEFLATED, deflate(Z, Bin)}.

unpack_deflated(Bin, Z) ->
    inflate(Z, Bin).

%%%----- Method DITTOFLATE: (experimental)
pack_dittoflate(Data, RefData, Z) ->
    {Part1,Part2} = dittoflate:compress(Z, Data, RefData),
    Part2P = dittoflate:pad(Part2),
    CompData = <<(byte_size(Part1)):32/unsigned, Part1/binary, Part2P/binary>>,
    {?METHOD_DITTOFLATE, CompData}.

%%%----- Method CHUNKED:
-define(CHUNK_METHOD_DEFLATE, 0).
-define(CHUNK_METHOD_PREFIX_COPY, 1).
-define(CHUNK_METHOD_OFFSET_COPY, 2).

unpack_chunked(<<>>, _RefData, _Z) ->
    <<>>;
unpack_chunked(<<?CHUNK_METHOD_DEFLATE:5, RSkipSpec:3/unsigned,
			CompSize:16/unsigned, CompData:CompSize/binary,
			Rest/binary>>, RefData, Z) ->
    {RefChunk, RestRefData} = do_rskip(RSkipSpec, RefData),
    DataChunk = inflate(Z, CompData, RefChunk),
    [DataChunk | unpack_chunked(Rest, RestRefData, Z)];
unpack_chunked(<<?CHUNK_METHOD_PREFIX_COPY:5, 0:3,
			2:16/unsigned, CopyLenM1:16/unsigned,
			Rest/binary>>, RefData, Z) ->
    CopyLen = CopyLenM1 + 1,
    {DataChunk, RestRefData} = erlang:split_binary(RefData, CopyLen),
    [DataChunk | unpack_chunked(Rest, RestRefData, Z)];
unpack_chunked(<<?CHUNK_METHOD_OFFSET_COPY:5, 0:3,
			4:16/unsigned, OffsetM1:16/unsigned, CopyLenM1:16/unsigned,
			Rest/binary>>, RefData, Z) ->
    CopyLen = CopyLenM1 + 1,
    Offset = OffsetM1 + 1,
    {_, OffsetRefData} = erlang:split_binary(RefData, Offset),
    {DataChunk, RestRefData} = erlang:split_binary(OffsetRefData, CopyLen),
    [DataChunk | unpack_chunked(Rest, RestRefData, Z)].

-record(deflate_option, {rskip_spec, dsize}).
-record(evaled_chunk_option, {ratio, chunk_method, comp_data, data_rest, ref_rest}).

pack_chunked(Data, RefData, Z) ->
    {?METHOD_CHUNKED, pack_chunked2(Data, RefData, Z)}.

pack_chunked2(<<>>, _RefData, _Z) ->
    <<>>;
pack_chunked2(Data, RefData, Z) ->
    Options = gen_chunk_deflate_options(byte_size(Data), byte_size(RefData)),
    EvaledOptions0 = lists:map(fun(Opt) -> evaluate_deflate_option(Opt, Data, RefData, Z) end,
			      Options),
    EvaledOptions = [evaluate_prefix_option(Data, RefData),
		     evaluate_offset_copy_option(Data, RefData)
		     | EvaledOptions0],
    SortedOptions = lists:keysort(#evaled_chunk_option.ratio, EvaledOptions),
    BestOption = hd(SortedOptions),
    #evaled_chunk_option{chunk_method=CM, comp_data=CompData, data_rest=DataRest, ref_rest=RefRest} = BestOption,
    
    CompSize = byte_size(CompData),
    (CompSize < 16#10000) orelse
	error({internal_error, compressed_to_large, byte_size(CompData)}),
%%     io:format("DB| chunk: method=~p size=~p\n", [CM, CompSize]),
    [CM, <<CompSize:16/unsigned>>, CompData
     | pack_chunked2(DataRest, RefRest, Z)].
    

gen_chunk_deflate_options(DataSz, RefDataSz) ->
    %% First choose RSkip, then choose DataSize:
    [#deflate_option{rskip_spec = RSkipSpec, dsize = DSize}
     || RSkipSpec <- [0,1,2,3],
	DSize <- begin
		     RSkip = spec_to_rskip(RSkipSpec),
		     AllRefDataVisible = (RefDataSz - RSkip =< ?WINDOW_SIZE) andalso (DataSz =< ?WINDOW_SIZE),
		     if AllRefDataVisible ->
			     %% All of RefData is visible.
			     %% Use the rest of the data (but ensure that
			     %% the output size fits in 16 bits)
			     [min(DataSz, ?LIMIT_SO_DEFLATED_FITS_IN_64KB)];
			true ->
			     [?CHUNK_SIZE,
			      (?CHUNK_SIZE * 3) div 2,
			      (?CHUNK_SIZE * 4) div 2]
		     end
		 end].

spec_to_dsize(DSizeSpec) -> (2+DSizeSpec) * (?CHUNK_SIZE div 2).

spec_to_rskip(RSkipSpec) -> RSkipSpec * (?CHUNK_SIZE div 2).

-define(DEFLATE_OVERHEAD_PENALTY_BYTES, 30).
-define(COPY_OVERHEAD_PENALTY_BYTES, 3).
evaluate_deflate_option(#deflate_option{rskip_spec = RSkipSpec, dsize = DSize}, Data, RefData, Z) ->
    {DataChunk, DataRest} = take_chunk(DSize, Data),
    {RefChunk, RestRefData} = do_rskip(RSkipSpec, RefData),

    CompData = deflate(Z, DataChunk, RefChunk),
    Ratio = (byte_size(CompData) + ?DEFLATE_OVERHEAD_PENALTY_BYTES) / byte_size(DataChunk),

    #evaled_chunk_option{ratio=Ratio,
			   chunk_method= <<?CHUNK_METHOD_DEFLATE:5, RSkipSpec:3>>,
			   comp_data=CompData,
			   data_rest=DataRest,
			   ref_rest=RestRefData}.

evaluate_prefix_option(Data, RefData) ->
    {Data2, _} = take_chunk(65536, Data),	% Limit common prefix to 64KB.
    PrefixLen = binary:longest_common_prefix([Data2, RefData]),

    {_Prefix,DataRest} = erlang:split_binary(Data, PrefixLen),
    {_,RestRefData} = erlang:split_binary(RefData, PrefixLen),
    CompData = <<(PrefixLen-1):16/unsigned>>,

    Ratio = if PrefixLen > 0 -> (byte_size(CompData) + ?COPY_OVERHEAD_PENALTY_BYTES) / PrefixLen;
	       true -> infinity
	    end,
    #evaled_chunk_option{ratio=Ratio,
			 chunk_method= <<?CHUNK_METHOD_PREFIX_COPY:5, 0:3>>,
			 comp_data=CompData,
			 data_rest=DataRest,
			 ref_rest=RestRefData}.

evaluate_offset_copy_option(Data, RefData) ->
    SuffixLen = binary:longest_common_suffix([Data, RefData]),
    DataSz    = byte_size(Data),
    RefDataSz = byte_size(RefData),
    Offset = RefDataSz - DataSz,
    if SuffixLen == DataSz, % All of Data is a suffix of RefData.
       SuffixLen > 0,
       Offset > 0,
       Offset =< 65536 ->
	    Len = min(65536, SuffixLen),
	    {_Prefix,DataRest} = erlang:split_binary(Data, Len),
	    {_,RestRefData} = erlang:split_binary(RefData, Offset + Len),
	    
	    CompData = <<(Offset-1):16/unsigned, (Len-1):16/unsigned>>,

	    Ratio = (byte_size(CompData) + 0) / Len,
       #evaled_chunk_option{ratio=Ratio,
			    chunk_method= <<?CHUNK_METHOD_OFFSET_COPY:5, 0:3>>,
			    comp_data=CompData,
			    data_rest=DataRest,
			    ref_rest=RestRefData};
       true ->
	    #evaled_chunk_option{ratio=infinity}
    end.


do_rskip(RSkipSpec, RefData) ->
    RSkip = spec_to_rskip(RSkipSpec),
    {_, RestRefData} = take_chunk(RSkip, RefData),
    {RefChunk, _} = take_chunk(?WINDOW_SIZE, RestRefData),
    {RefChunk, RestRefData}.    

%%%----- Method CHUNKED_MIDDLE:
pack_chunked_middle(Data, RefData, Z) ->
%%     DataLen = byte_size(Data),
%%     RefLen  = byte_size(RefData),

    %% Calculate and remove prefix:
    PrefixLen = binary:longest_common_prefix([Data, RefData]),
    <<_:PrefixLen/binary, DataSansPrefix/binary>> = Data,
    <<_:PrefixLen/binary, RefSansPrefix/binary >> = RefData,

    %% Calculate and remove suffix:
    SuffixLen = binary:longest_common_suffix([DataSansPrefix, RefSansPrefix]),
    DataMidLen = byte_size(DataSansPrefix) - SuffixLen,
    RefMidLen  = byte_size(RefSansPrefix)  - SuffixLen,
    <<DataMiddle:DataMidLen/binary, _:SuffixLen/binary>> = DataSansPrefix,
    <<RefMiddle:RefMidLen/binary,   _:SuffixLen/binary>> = RefSansPrefix,
    
    CompMiddle = pack_chunked2(DataMiddle, RefMiddle, Z),
    {?METHOD_CHUNKED_MIDDLE,
     [<<PrefixLen:32/unsigned, SuffixLen:32/unsigned>>, CompMiddle]}.

unpack_chunked_middle(CompData, RefData, Z) ->
    <<PrefixLen:32/unsigned, SuffixLen:32/unsigned, CompMiddle/binary>> = CompData,
    RefMidLen  = byte_size(RefData) - PrefixLen - SuffixLen,
    <<Prefix:PrefixLen/binary, RefMiddle:RefMidLen/binary, Suffix:SuffixLen/binary>> = RefData,
    [Prefix, unpack_chunked(CompMiddle, RefMiddle, Z), Suffix].


%%%-------------------- Utility -------------------

take_chunk(MaxSize, Bin) when MaxSize > byte_size(Bin) ->
    {Bin,<<>>};
take_chunk(MaxSize, Bin) when MaxSize =< byte_size(Bin) ->
    <<Chunk:MaxSize/binary, Rest/binary>> = Bin,
    {Chunk, Rest}.

%% -ifdef(EXCLUDE_ZLIB_HEADERS).
-define(ZLIB_WINDOW_SIZE_BITS, -15).
%% -else.
%% -define(ZLIB_WINDOW_SIZE_BITS, 15).
%% -endif.

deflate(Z, Data, RefData) ->
    zlib:deflateInit(Z, best_compression, deflated, ?ZLIB_WINDOW_SIZE_BITS, 9, default),
    zlib:deflateSetDictionary(Z, RefData),
    CompData = iolist_to_binary(zlib:deflate(Z, Data, finish)),
    zlib:deflateEnd(Z),
    CompData.

deflate(Z, Data) ->
    zlib:deflateInit(Z, best_compression, deflated, ?ZLIB_WINDOW_SIZE_BITS, 9, default),
    CompData = iolist_to_binary(zlib:deflate(Z, Data, finish)),
    zlib:deflateEnd(Z),
    CompData.
    
-ifdef(EXCLUDE_ZLIB_HEADERS).
inflate(Z, CompData, RefData) ->
    zlib:inflateInit(Z, ?ZLIB_WINDOW_SIZE_BITS),

    zlib:inflateSetDictionary(Z, RefData),
    Data = zlib:inflate(Z, CompData),

    zlib:inflateEnd(Z),
    Data.

-else.
inflate(Z, CompData, RefData) ->
    zlib:inflateInit(Z, ?ZLIB_WINDOW_SIZE_BITS),

    %% When zlib headers are on, inflateSetDictionary() can only be called after a failed inflate().
    Data = try zlib:inflate(Z, CompData)
	   catch error:{need_dictionary, _} ->
		   zlib:inflateSetDictionary(Z, RefData),
		   zlib:inflate(Z, CompData)
	   end
    end,
    zlib:inflateEnd(Z),
    Data.
-endif.
    
inflate(Z, CompData) ->
    zlib:inflateInit(Z, ?ZLIB_WINDOW_SIZE_BITS),
    Data = zlib:inflate(Z, CompData),
    zlib:inflateEnd(Z),
    Data.
