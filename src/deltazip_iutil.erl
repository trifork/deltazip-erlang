-module(deltazip_iutil).
-export([varlen_encode/1, varlen_decode/1]).
-export([deflate/2, deflate/3, inflate/2, inflate/3]).

%%% DeltaZip internal utilities.
% Purpose: General-ish functions for internal use.


%%========== Var-length integer encoding:

varlen_encode(N) when N >= 0 ->
    varlen_encode(N, 0).
varlen_encode(N, Flag) ->
    Byte = (N band 16#7F) bor (Flag bsl 7),
    Rest = N bsr 7,
    if Rest == 0 ->
	    [Byte];
       true ->
	    [varlen_encode(Rest, 1), Byte]
    end.

varlen_decode(Bin) -> varlen_decode(Bin, 0).
varlen_decode(<<Flag:1, Bits:7, Rest/binary>>, Acc) ->
    Acc2 = (Acc bsl 7) + Bits,
    if Flag==0 -> {Acc2, Rest};
       true    -> varlen_decode(Rest, Acc2)
    end.

%%========== Deflate/Inflate compression:

-define(ZLIB_WINDOW_SIZE_BITS, -15). % 32KB window, no ZLIB headers.

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

inflate(Z, CompData, RefData) ->
    zlib:inflateInit(Z, ?ZLIB_WINDOW_SIZE_BITS),

    zlib:inflateSetDictionary(Z, RefData),
    Data = zlib:inflate(Z, CompData),

    zlib:inflateEnd(Z),
    Data.

%%% Lesson learned: when zlib headers are on, inflateSetDictionary()
%%% can only be called after a failed inflate().  Just in case we're going to
%%% include zlib headers again at some point.

inflate(Z, CompData) ->
    zlib:inflateInit(Z, ?ZLIB_WINDOW_SIZE_BITS),
    Data = zlib:inflate(Z, CompData),
    zlib:inflateEnd(Z),
    Data.



-ifdef(TEST). %============================================================
-include_lib("eunit/include/eunit.hrl").

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

inflate_deflate_test() ->
    Z = zlib:open(),
    Inputs = [crypto:rand_bytes(N) || N <- lists:seq(0,1000)],
    lists:foreach(fun(In) ->
                          Comp = deltazip_iutil:deflate(Z, In),
                          Out = deltazip_iutil:inflate(Z, Comp),
                          ?assertEqual(In, iolist_to_binary(Out))
                  end,
                  Inputs).

inflate_deflate_with_reference_test() ->
    Z = zlib:open(),
    Inputs = [{crypto:rand_bytes(10*N),crypto:rand_bytes(10*M)}
              || N <- lists:seq(0,40), M <- lists:seq(0,40)],
    lists:foreach(fun({Ref,In}) ->
                          Comp = deltazip_iutil:deflate(Z, In, Ref),
                          Out = deltazip_iutil:inflate(Z, Comp, Ref),
                          ?assertEqual(In, iolist_to_binary(Out))
                  end,
                  Inputs).

-endif.

