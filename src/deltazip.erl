-module(deltazip).
-compile(export_all).

-export([open/1, get/1, previous/1, add/2, add_multiple/2, close/1]).
-export([stats_for_current_entry/1]).
-export([main/1]).

-import(deltazip_util, [varlen_encode/1, varlen_decode/1]).

-type file_format_version() :: {Major::byte(), Minor::byte()}.
-type get_size_fun() :: fun(() -> integer()).
-type pread_fun() :: fun((integer(),integer()) -> {ok, binary()} | {error,_}).
-type file_tail() :: {Pos::integer(), Tail::binary()}.

-record(dzstate, {get_size_fun :: get_size_fun(),
		  pread_fun    :: pread_fun(),
                  format_version :: file_format_version(),
		  zip_handle,
		  file_header_state :: empty | valid,
		  current_pos :: integer(),
		  current_size :: integer(),
		  current_method :: integer(),
		  current_version :: binary() | file_is_empty,
		  current_checksum :: integer()
		 }).

-define(DELTAZIP_MAGIC_HEADER, 16#CEB47A).
-define(DEFAULT_VERSION_MAJOR, 1).
-define(DEFAULT_VERSION_MINOR, 1).

-define(FILE_HEADER_LENGTH, 4).

%% Snapshot methods (0-3):
-define(METHOD_UNCOMPRESSED,   0).
-define(METHOD_DEFLATED,       1).
%% Delta methods (4-15):
-define(METHOD_CHUNKED,        4).
-define(METHOD_CHUNKED_MIDDLE, 5).
-define(METHOD_DITTOFLATE,     6). % Experimental
-define(METHOD_CHUNKED_MIDDLE2, 7).

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

%%%-------------------- ESCRIPT --------------------
-spec main/1 :: ([string()]) -> any().
main(Args) -> deltazip_cli:main(Args).

%%%-------------------- API --------------------

-spec open/1 :: ({get_size_fun(), pread_fun()}) -> #dzstate{} | {error,_}.
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

-spec get/1 :: (#dzstate{}) -> binary() | file_is_empty.
get(#dzstate{current_version=Bin}) ->
%%     io:format("DB| get @ ~p: ~p\n", [S#dzstate.current_pos, Bin]), 
    Bin.

-spec stats_for_current_entry/1 :: (#dzstate{}) -> {integer(), integer(), integer()}.
stats_for_current_entry(#dzstate{current_method=M,
				 current_size=Sz,
				 current_checksum=Ck}) ->
    {M, Sz, Ck}.

-spec(previous/1 :: (#dzstate{}) -> {ok, #dzstate{}} | {error, at_beginning}).
previous(#dzstate{current_pos=CP}) when CP =< ?FILE_HEADER_LENGTH ->
    {error, at_beginning};
previous(State) ->
    {ok, compute_current_version(goto_previous_position(State))}.

-spec add/2 :: (#dzstate{}, binary()) -> file_tail().
add(State, NewRev) ->
    add_multiple(State, [NewRev]).

-spec add_multiple/2 :: (#dzstate{}, [binary()]) -> file_tail().
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

-spec close/1 :: (#dzstate{}) -> any().
close(#dzstate{zip_handle=Z}) ->
    zlib:close(Z).

%%%-------------------- Implementation --------------------

check_magic_header(State) ->
    case magic_header_state(State) of
	invalid ->
            error(not_a_deltazip_file),
            HeaderState = FormatVersion = dummy;
        empty ->
            HeaderState = empty,
            FormatVersion = {?DEFAULT_VERSION_MAJOR, ?DEFAULT_VERSION_MINOR};
        {valid, Major, Minor} ->
            verify_deltazip_version_is_supported(Major, Minor),
            HeaderState = valid,
            FormatVersion = {Major, Minor}
    end,
    State#dzstate{file_header_state=HeaderState,
                  format_version=FormatVersion}.

verify_deltazip_version_is_supported(Major, Minor) ->
    if Major == 1,
       Minor >= 0, Minor =< 1 ->
            ok;
       true ->
            error({unsupported_deltazip_version, Major, Minor})
    end.


-spec(magic_header_state/1 :: (#dzstate{}) -> empty | invalid | {valid,byte(),byte()}).
magic_header_state(#dzstate{get_size_fun=GetSizeFun,
			    pread_fun=PReadFun}) ->
    Size = GetSizeFun(),
    if Size == 0 ->
	    empty;
       Size < 4 ->
	    invalid;
       Size >= 4 ->
	    case PReadFun(0,4) of
		{ok,<<Magic:24, Major:4, Minor:4>>}
                  when Magic == ?DELTAZIP_MAGIC_HEADER ->
		    {valid, Major, Minor};
		_ ->
		    invalid
	    end
    end.

set_initial_position(State=#dzstate{get_size_fun=GetSizeFun}) ->
    State#dzstate{current_pos=GetSizeFun()}.

-define(ENVELOPE_OVERHEAD, (4+4+4)). % Start-tag, checksum, end-tag.
goto_previous_position(State=#dzstate{current_pos=Pos}) ->
    <<Method:4, Size:28>>=Tag = safe_pread(State, Pos-4, 4),
    PrevPos = Pos - Size - ?ENVELOPE_OVERHEAD,
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
    <<Adler32:32/unsigned, Bin/binary>> = safe_pread(State, Pos+4, Size+4),
    Version = unpack_entry(State, Method,Bin),
    ActualAdler = erlang:adler32(Version),
    if ActualAdler /= Adler32 -> error({checksum_mismatch,Adler32,ActualAdler});
       true -> ok
    end,
    State#dzstate{current_version = Version,
		  current_checksum = ActualAdler}.

pack_multiple([], _Z) ->
    [];
pack_multiple([Data], Z) ->
    envelope(Data, select_method_and_pack_snapshot(Data, Z));
pack_multiple([Data | [NextData|_]=Rest], Z) ->
    [envelope(Data, select_method_and_pack_delta(Data, NextData, Z))
     | pack_multiple(Rest, Z)].

opt_prepend_file_header_to_append_spec(State, AppendSpec={Pos,Tail}) ->
    case State#dzstate.file_header_state of
	valid -> AppendSpec;
	empty when Pos == 0 ->
            {Pos, [<<?DELTAZIP_MAGIC_HEADER:24/big,
                     ?DEFAULT_VERSION_MAJOR:4, ?DEFAULT_VERSION_MINOR:4>>
                       | Tail]}
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

envelope(RawVersion, {Method, Data0}) ->
    Data = iolist_to_binary(Data0),
    Sz = byte_size(Data),
%%     io:format("DB| envelope: method=~p  size=~p\n", [Method, Sz]),
    Tag = <<Method:4, Sz:28>>,
    Adler32 = <<(erlang:adler32(RawVersion)):32>>,
    [Tag, Adler32, Data, Tag].

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
    unpack_chunked_middle(Data, State#dzstate.current_version, State#dzstate.zip_handle);
unpack_entry_to_iolist(State, ?METHOD_CHUNKED_MIDDLE2, Data) ->
    unpack_chunked_middle2(Data, State#dzstate.current_version, State#dzstate.zip_handle).

select_method_and_pack_snapshot(Data, Z) ->
    pack_deflated(Data, Z).

select_method_and_pack_delta(Data, RefData, Z) ->
    AllowDitto = erlang:get(allow_dittoflate) /= undefined,
    ForceDitto = erlang:get(force_dittoflate) /= undefined,
    Methods = if ForceDitto ->
		      [fun pack_dittoflate/3];
		 true ->
		      [%fun pack_chunked/3,
		       fun pack_chunked_middle/3,
		       fun pack_chunked_middle2/3
		      ]
			  ++ [fun pack_dittoflate/3 || AllowDitto]
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
    BestOption = decide_chunk_method_for_next_chunk(Data, RefData, Z),
    #evaled_chunk_option{chunk_method=CM, comp_data=CompData, data_rest=DataRest, ref_rest=RefRest} = BestOption,
    
    CompSize = byte_size(CompData),
    (CompSize < 16#10000) orelse
	error({internal_error, compressed_to_large, byte_size(CompData)}),
%%     io:format("DB| chunk: method=~p size=~p\n", [CM, CompSize]),
    [CM, <<CompSize:16/unsigned>>, CompData
     | pack_chunked2(DataRest, RefRest, Z)].
    
decide_chunk_method_for_next_chunk(Data, RefData, Z) ->
    Options = gen_chunk_deflate_options(byte_size(Data), byte_size(RefData)),
    EvaledOptions0 = lists:map(fun(Opt) -> evaluate_deflate_option(Opt, Data, RefData, Z) end,
			      Options),
    EvaledOptions = [evaluate_prefix_option(Data, RefData, Z),
		     evaluate_offset_copy_option(Data, RefData, Z)
		     | EvaledOptions0],
    SortedOptions = lists:keysort(#evaled_chunk_option.ratio, EvaledOptions),
    hd(SortedOptions).

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

evaluate_prefix_option(Data, RefData, _Z) ->
    {Data2, _} = take_chunk(65536, Data),	% Limit common prefix to 64KB.
    PrefixLen = binary:longest_common_prefix([Data2, RefData]),

    if PrefixLen > 0 ->
	    {_Prefix,DataRest} = erlang:split_binary(Data, PrefixLen),
	    {_,RefRest} = erlang:split_binary(RefData, PrefixLen),

	    CompData = <<(PrefixLen-1):16/unsigned>>,

	    Ratio = (byte_size(CompData) + ?COPY_OVERHEAD_PENALTY_BYTES) / PrefixLen,
	    #evaled_chunk_option{ratio=Ratio,
				 chunk_method= <<?CHUNK_METHOD_PREFIX_COPY:5, 0:3>>,
				 comp_data=CompData,
				 data_rest=DataRest,
				 ref_rest=RefRest};
       true ->
	    #evaled_chunk_option{ratio=infinity}
    end.

evaluate_offset_copy_option(Data, RefData, _Z) ->
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
	    {_,RefRest} = erlang:split_binary(RefData, Offset + Len),
	    
	    CompData = <<(Offset-1):16/unsigned, (Len-1):16/unsigned>>,

	    Ratio = (byte_size(CompData) + 0) / Len,
	    #evaled_chunk_option{ratio=Ratio,
				 chunk_method= <<?CHUNK_METHOD_OFFSET_COPY:5, 0:3>>,
				 comp_data=CompData,
				 data_rest=DataRest,
				 ref_rest=RefRest};
       true ->
	    #evaled_chunk_option{ratio=infinity}
    end.


do_rskip(RSkipSpec, RefData) ->
    RSkip = spec_to_rskip(RSkipSpec),
    {_, RestRefData} = take_chunk(RSkip, RefData),
    {RefChunk, _} = take_chunk(?WINDOW_SIZE, RestRefData),
    {RefChunk, RestRefData}.    

%%%----- Method CHUNKED_MIDDLE:
-define(CONTEXT_PRIORITIZE_PAST,   1).
-define(CONTEXT_PRIORITIZE_FUTURE, 2).

pack_chunked_middle(Data, RefData, Z) ->
    {PrefixLen, SuffixLen, DataMiddle, _, RefMiddle, _} =
	identify_middle(Data, RefData),
    CompMiddle = pack_chunked2(DataMiddle, RefMiddle, Z),
    {?METHOD_CHUNKED_MIDDLE,
     [varlen_encode(PrefixLen), varlen_encode(SuffixLen), CompMiddle]}.

pack_chunked_middle2(Data, RefData, Z) ->
    {PrefixLen, SuffixLen, DataMiddle, RefPre, RefMiddle, RefSuf} =
	identify_middle(Data, RefData),
    {PrePart, _} = take_end_chunk(?WINDOW_SIZE div 2, RefPre),
    RefToUse = <<PrePart/binary, RefMiddle/binary, RefSuf/binary>>,
    CompMiddle = pack_chunked2(DataMiddle, RefToUse, Z),
    {?METHOD_CHUNKED_MIDDLE2,
     [varlen_encode(PrefixLen), varlen_encode(SuffixLen), CompMiddle]}.

identify_middle(Data, RefData) ->
    %% Calculate prefix and suffix lengths:
    PrefixLen = binary:longest_common_prefix([Data, RefData]),
    <<_:PrefixLen/binary, DataSansPrefix/binary>> = Data,
    <<RefPrefix:PrefixLen/binary, RefSansPrefix/binary >> = RefData,
    SuffixLen = binary:longest_common_suffix([DataSansPrefix, RefSansPrefix]),

    %% Take middle:
    {_, DataMiddle,_} = take_middle(Data, PrefixLen, SuffixLen),
    {RefPrefix, RefMiddle, RefSuffix} = take_middle(RefData, PrefixLen, SuffixLen),

    {PrefixLen, SuffixLen, DataMiddle, RefPrefix, RefMiddle, RefSuffix}.

take_middle(Bin, PrefixLen, SuffixLen) ->
    MidLen  = byte_size(Bin) - PrefixLen - SuffixLen,
    <<Prefix:PrefixLen/binary, Middle:MidLen/binary,  Suffix:SuffixLen/binary >> = Bin,
    {Prefix, Middle, Suffix}.

-define(unstream_1_of_2(Var, Expr), element(2, begin {Var,_} = (Expr) end)).
unpack_chunked_middle(CompData, RefData, Z) ->
    CompMiddle = ?unstream_1_of_2(SuffixLen, varlen_decode
				  (?unstream_1_of_2(PrefixLen, varlen_decode
						    (CompData)))),
    {Prefix, RefMiddle, Suffix} = take_middle(RefData, PrefixLen, SuffixLen),
    [Prefix, unpack_chunked(CompMiddle, RefMiddle, Z), Suffix].


unpack_chunked_middle2(CompData, RefData, Z) ->
    CompMiddle = ?unstream_1_of_2(SuffixLen, varlen_decode
				  (?unstream_1_of_2(PrefixLen, varlen_decode
						    (CompData)))),
    {Prefix, RefMiddle, Suffix} = take_middle(RefData, PrefixLen, SuffixLen),
    {PrePart, _} = take_end_chunk(?WINDOW_SIZE div 2, Prefix),
    RefToUse = <<PrePart/binary, RefMiddle/binary, Suffix/binary>>,
    [Prefix, unpack_chunked(CompMiddle, RefToUse, Z), Suffix].


%%%-------------------- Utility -------------------

take_chunk(MaxSize, Bin) when MaxSize >= byte_size(Bin) ->
    {Bin,<<>>};
take_chunk(MaxSize, Bin) when MaxSize < byte_size(Bin) ->
    <<Chunk:MaxSize/binary, Rest/binary>> = Bin,
    {Chunk, Rest}.

take_end_chunk(MaxSize, Bin) when MaxSize >= byte_size(Bin) ->
    {Bin,<<>>};
take_end_chunk(MaxSize, Bin) when MaxSize < byte_size(Bin) ->
    SkipAmount = byte_size(Bin) - MaxSize,
    <<Rest:SkipAmount/binary, Chunk:MaxSize/binary>> = Bin,
    {Chunk, Rest}.

%%--------------------

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

    
-ifdef(TEST). %============================================================
varlen_test() ->
    <<0>> = iolist_to_binary(varlen_encode(0)),
    <<127>> = iolist_to_binary(varlen_encode(127)),
    <<129,0>> = iolist_to_binary(varlen_encode(128)),
    <<129,128, 0>> = iolist_to_binary(varlen_encode(1 bsl 14)),
    
    lists:foreach(fun(N) ->
			  io:format("varlen_test: ~p\n", [N]),
			  Bin = iolist_to_binary([varlen_encode(N), "Rest"]),
			  {N, <<"Rest">>} = varlen_decode(Bin)
		  end,
		  [0,1, 127,128, 255,256, 1023,1024, 2000, 4000,
		   60000, 80000, 1000000, 4000000000, 8000000000]),
    ok.
    
-endif.
