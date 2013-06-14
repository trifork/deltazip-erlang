-module(deltazip_util).
-compile(export_all).

%%========== File access:
fd_access(Fd) ->
    GetSizeFun = fun() -> {ok, Pos} = file:position(Fd, eof), Pos end,
    PReadFun   = fun(Pos,Size) ->
			 {ok, Data} = file:pread(Fd, Pos, Size),
			 {ok, Data}
		 end,
    {GetSizeFun, PReadFun}.


%%========== Binary access:

bin_access(Bin) ->
    GetSizeFun = fun() -> byte_size(Bin) end,
    PReadFun   = fun(Pos,Size) ->
			 <<_:Pos/binary, Data:Size/binary, _/binary>> = Bin,
			 {ok, Data}
		 end,
    {GetSizeFun, PReadFun}.

	     
replace_tail(Bin, {PrefixLength, NewTail}) when is_integer(PrefixLength), (is_binary(NewTail) or is_list(NewTail)) ->
    NewTailBin = iolist_to_binary(NewTail),
    <<Prefix:PrefixLength/binary, _/binary>> = Bin,
    <<Prefix/binary, NewTailBin/binary>>.


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
