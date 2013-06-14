-module(dittoflate).
-compile(export_all).

%%% Delta-compression, using a dedicated decompression machine
%%% and zlib for huffman-encoding.
%%% This is a prototype - focus is on simplicity and experimentation rather than performance.

%%% EXPERIMENTAL!


compress(Z, Data, RefData) ->
    Spec = compress_to_spec(Data, RefData),
%%     io:format("SPEC: ~p\n", [Spec]),
    {Instrs0,Args0} = spec_to_streams(Spec, <<>>, <<>>),
    Instrs = iolist_to_binary(Instrs0),
    Args   = iolist_to_binary(Args0),
    {deflate(Z,Instrs), Args}.

-define(CODE_BASE_PAST,     (0*32)).
-define(CODE_BASE_ORIGINAL, (1*32)).
-define(CODE_BASE_DISCARDS, (2*32)).
-define(CODE_BASE_LITERAL,  (3*32)).

spec_to_streams([], Instrs, Args) -> {Instrs, Args};
spec_to_streams([{comment,_} | Rest], Instrs, Args) ->
    spec_to_streams(Rest, Instrs, Args);
%% spec_to_streams([{lit,X} | Rest], Instrs, Args) ->
%%     spec_to_streams(Rest, <<Instrs/binary, 0, X>>, Args);
spec_to_streams([{lit,X} | Rest]=L, Instrs, Args) ->
    {Lits, Rest2} = collect_literals(L),
    Count = length(Lits),
%%     io:format("DB| LitCount=~p\n", [Count]),
    if Count>=3 ->
	    LitsBin = list_to_binary(Lits),
	    {Code1, NCntBits, CntBits} = length_to_code(Count),
	    spec_to_streams(Rest2,
			    <<Instrs/binary, (?CODE_BASE_LITERAL+Code1), LitsBin/binary>>,
			    <<Args/bitstring, CntBits:NCntBits>>);
       true ->
	    spec_to_streams(Rest, <<Instrs/binary, 0, X>>, Args)
    end;
spec_to_streams([{W,Len,Off} | Rest], Instrs, Args) ->
    CodeBase = case W of
		   past     -> DBase=1, ?CODE_BASE_PAST;
		   original -> DBase=0, ?CODE_BASE_ORIGINAL;
		   discards -> DBase=0, ?CODE_BASE_DISCARDS
	       end,
    {Code1, NLenBits, LenBits} = length_to_code(Len),
    {Code2, NOffBits, OffBits} = distance_to_code(Off-DBase+1),
    spec_to_streams(Rest, <<Instrs/binary, (CodeBase+Code1), Code2>>, <<Args/bitstring, LenBits:NLenBits, OffBits:NOffBits>>).
    

collect_literals(L) ->
    {Lits,Rest} = lists:splitwith(fun(X) ->
					  Elm=element(1,X),
					  Elm==lit orelse Elm==comment
				  end, L),
    {lists:flatmap(fun({lit,X})->[X];
		      ({comment,_})->[] end,
		   Lits),
     Rest}.

%% From RFC 1951 (deflate):
-define(LENGTH_TO_CODE_SPEC,
    [0, 0, 0, 0, 0, 0, 0, 0,
     1, 1, 1, 1,
     2, 2, 2, 2,
     3, 3, 3, 3,
     4, 4, 4, 4,
     5, 5, 5, 5,
     16, %% Added.
     0]).
length_to_code(L) when L>=3, L=<16#10000 ->
    integer_to_code(L-3, ?LENGTH_TO_CODE_SPEC, 0).

-define(DISTANCE_TO_CODE_SPEC,
    [0, 0, 0, 0,
     1, 1, 2, 2,
     3, 3, 4, 4,
     5, 5, 6, 6,
     7, 7, 8, 8,
     9, 9, 10, 10,
     11, 11, 12, 12,
     13, 13,
     16, 24, 32]).
distance_to_code(D) when D>=1 -> %, D=<16#8000 ->
    integer_to_code(D-1, ?DISTANCE_TO_CODE_SPEC, 0).

integer_to_code(L, [NBits|Rest], Code) ->
    L2 = L - (1 bsl NBits),
    if L2 < 0 -> % L fits into NBits bits
	    {Code, NBits, L};
       true ->
	    integer_to_code(L2, Rest, Code + 1)
    end.

compress_to_spec(Data, RefData) ->
    compress_to_spec(Data, <<>>, RefData, <<>>).

compress_to_spec(<<>>, _PastWindow, _FwdWindow, _DiscardWindow) ->
    [];
compress_to_spec(Data, PastWindow, FwdWindow, DiscardWindow) ->
    case find_in_windows(Data, PastWindow, FwdWindow, DiscardWindow) of
	no_match ->
	    Item = {lit, binary:at(Data,0)},
	    EatAmount = 1,
	    OrgSkip = none;
	{w1, Len, Off0} ->
	    Off = byte_size(PastWindow)-Off0,
	    if Off==Len -> % Past up until now
		    {_,Data2} = eat(Data,Len),
		    FutureToo = binary:longest_common_prefix([Data, Data2]);
	       true ->
		    FutureToo = 0
	    end,
	    TotalLen = Len + FutureToo,
	    Item = {past, TotalLen, Off},
	    EatAmount = TotalLen,
	    OrgSkip = none;
	{w2, Len, Off} ->
	    Item = {original, Len, Off},
	    EatAmount = Len,
	    OrgSkip = {Len,Off};
	{w3, Len, Off0} ->
	    Off = byte_size(DiscardWindow)-Off0,
	    Item = {discards, Len, Off},
	    EatAmount = Len,
	    OrgSkip = none
    end,
    {Eaten, Rest} = eat(Data, EatAmount),

    PastWindow2 = append_to_window(PastWindow, Eaten, 16#8000),

    case OrgSkip of
	none ->
	    FwdWindow2 = FwdWindow,
	    DiscardWindow2 = DiscardWindow;
	{OrgSkipLen, OrgSkipOff} ->
	    {OrgData, FwdWindow2} = eat(FwdWindow, OrgSkipOff+OrgSkipLen),
	    {Discard, _} = eat(OrgData, OrgSkipOff),
	    DiscardWindow2 = append_to_window(DiscardWindow, Discard, 16#8000)
    end,

    [Item
%%      , {comment, Eaten}
     | compress_to_spec(Rest, PastWindow2, FwdWindow2, DiscardWindow2)]. % TODO: Fwd/discard

eat(Data, Len) ->
    <<Eaten:Len/binary, Rest/binary>> = Data,
    {Eaten, Rest}.
    

append_to_window(Window, Extra, WSize) ->
    Window2 = <<Window/binary, Extra/binary>>,
    DelAmount = max(0, (byte_size(Window2) - WSize)),
    <<_:DelAmount/binary, Window3/binary>> = Window2,
    Window3.
    

find_in_windows(Data, PastWindow, FwdWindow, DiscardWindow) ->
    case binary:longest_common_prefix([Data, FwdWindow]) of
	CopyLen when CopyLen >= 3 ->
	    {w2, CopyLen, 0};
	_ ->
	    M1 = {M1Len,_} = find_last_longest_match(Data, PastWindow),
	    M2 = {M2Len,M2Off} = find_first_longest_match(Data, FwdWindow),
	    M3 = {M3Len,_} = find_last_longest_match(Data, DiscardWindow),
	    M2Len2 = M2Len - (M2Off div 100), % Punish early skipping.
	    %%     io:format("Search results: ~p ~p ~p\n", [M1, M2, M3]),
	    {_, {BestLen, BestOffset}, WinID} = _BestMatch =
		lists:last(lists:sort([{M1Len,  M1, w1},
				       {M2Len2, M2, w2},
				       {M3Len,  M3, w3}])),
	    if BestLen < 1 -> no_match;
	       true -> {WinID, BestLen, BestOffset}
	    end
    end.

find_first_longest_match(Data, Window) ->
    find_longest_match(Data, 3, Window, 0, -1, -1, 1).

find_last_longest_match(Data, Window) ->
    find_longest_match(Data, 3, Window, 0, -1, -1, 0).

find_longest_match(Data, NeedleLen, Window, Base, BestLength, BestOffset, Skip)
  when byte_size(Window)>Base+NeedleLen, byte_size(Data) >= NeedleLen ->
%%     io:format("FLM: ~p\n", [{Data, NeedleLen, Window, Base, BestLength, BestOffset, Skip}]),
    <<Needle:NeedleLen/binary, _/binary>> = Data,
    case binary:match(Window, Needle, [{scope, {Base, byte_size(Window)-Base}}]) of
	nomatch ->
	    {BestLength, BestOffset};
	{FoundOffset, _FoundLength} ->
	    <<_:FoundOffset/binary, WindowTail/binary>> = Window,
	    MatchLen = binary:longest_common_prefix([Data, WindowTail]),
%% 	    io:format("R=~p for ~p / ~p / ~p\n", [R, Window, Needle, [{scope, {Base, byte_size(Window)-Base}}]]),
%% 	    io:format("FLM: MatchLen=~p, Tail=~p, W=~p, Base=~p, FO=~p\n", [MatchLen, WindowTail, Window, Base, FoundOffset]),
	    NewLen = MatchLen + Skip,
	    find_longest_match(Data, NewLen, Window, FoundOffset+1, MatchLen, FoundOffset, Skip)
    end;
find_longest_match(_Data, _NeedleLen, _Window, _Base, BestLength, BestOffset, _Skip) ->
    {BestLength, BestOffset}.
   

deflate(Z, Data) ->
    zlib:deflateInit(Z, best_compression, deflated, -15, 9, huffman_only),
    CompData = iolist_to_binary(zlib:deflate(Z, Data, finish)),
    zlib:deflateEnd(Z),
    CompData.

pad(Data) ->
    PadBits = 7-((7+bit_size(Data)) rem 8),
    <<Data/bitstring, 0:PadBits>>.


%%%====================
test() ->
    {w2, 5, 4} = find_in_windows(<<"Hello, World">>, <<"W1: ell">>, <<"W2: Hello">>, <<"W3: H">>),
    {w1, 3, 5} = find_in_windows(<<"Hello, World">>, <<"W1: xHel">>, <<"W2: He-llo">>, <<"W3: H">>),

    [{original, 5, 0}, {lit, $!}] =
	compress_to_spec(<<"Hello!">>, <<"Hello, World!">>),

    [{lit, $X}, {lit, $y}, {lit, $x}, {lit, $y}, {lit, $x}, {past, 4,4}, {lit, $Z}] =
	compress_to_spec(<<"XyxyxyxyxZ">>, <<>>),
    
    [{original, 3,4}, {lit, 32}, {discards, 3,4}] = 
	compress_to_spec(<<"abc def">>, <<"def abc">>).

len2code_test() ->
    {0,0,0} = length_to_code(3),
    {7,0,0} = length_to_code(10),
    {8,1,0} = length_to_code(11),
    {8,1,1} = length_to_code(12),
    {11,1,1} = length_to_code(18),
    {12,2,0} = length_to_code(19),
    {15,2,3} = length_to_code(34),
    {16,3,0} = length_to_code(35),
    {27,5,30} = length_to_code(257),
    %% TODO: 258 should yield 0-bit encoding.
    ok.
    
dist2code_test() ->
    {0,0,0} = distance_to_code(1),
    {3,0,0} = distance_to_code(4),
    {9,3,0} = distance_to_code(25),
    {9,3,7} = distance_to_code(32),
    {29,13,0} = distance_to_code(24577),
    %% TODO: 258 should yield 0-bit encoding.
    ok.
    
