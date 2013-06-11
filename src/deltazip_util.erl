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
