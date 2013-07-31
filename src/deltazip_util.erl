-module(deltazip_util).
-compile(export_all).

-include("../include/deltazip.hrl").

%%========== File access:
file_access(Fd) ->
    GetSizeFun = fun() -> {ok, Pos} = file:position(Fd, eof), Pos end,
    PReadFun   = fun(Pos,Size) ->
			 {ok, Data} = file:pread(Fd, Pos, Size),
			 {ok, Data}
		 end,
    ReplaceTailFun = fun(Pos,NewTail) ->
                             %% Write the new tail:
                             ok = file:pwrite(Fd, Pos, NewTail),
                             %% Truncate: (alas, there is no file:truncate/2)
                             {ok,_} = file:position(Fd, Pos + iolist_size(NewTail)),
                             ok = file:truncate(Fd)
                             %% TODO: this could have better error handling.
                     end,
    #dz_access{get_size=GetSizeFun, pread=PReadFun, replace_tail=ReplaceTailFun}.


%%========== Binary access:

bin_access(Bin) when is_binary(Bin) ->
    GetSizeFun = fun() -> byte_size(Bin) end,
    PReadFun   = fun(Pos,Size) ->
			 <<_:Pos/binary, Data:Size/binary, _/binary>> = Bin,
			 {ok, Data}
		 end,
    ReplaceTailFun = fun(Pos,NewTail) ->
                             NewTailBin = iolist_to_binary(NewTail),
                             <<Prefix:Pos/binary, _/binary>> = Bin,
                             <<Prefix/binary, NewTailBin/binary>>
                     end,
    #dz_access{get_size=GetSizeFun, pread=PReadFun, replace_tail=ReplaceTailFun}.

