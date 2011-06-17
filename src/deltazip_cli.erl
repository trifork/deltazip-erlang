-module(deltazip_cli).

-export([main/1]).

main(["create", DZFile | InputFiles]) ->
    do_create(DZFile, InputFiles),
    init:stop().

do_create(DZFile, InputFiles) ->
    {ok, Fd} = file:open(DZFile, [write, exclusive, binary, raw]),
    Access = fd_access(Fd),
    DZ = deltazip:open(Access),
    Datas = [begin {ok,D} = file:read_file(F), D end
	     || F <- InputFiles],
    {0, Data} = deltazip:add_multiple(DZ, Datas),
    ok = file:write(Fd, Data),
    ok = file:close(Fd).

fd_access(Fd) ->
    GetSizeFun = fun() -> {ok, Pos} = file:position(Fd, eof), Pos end,
    PReadFun   = fun(Pos,Size) ->
			 {ok, Data} = file:pread(Fd, Pos, Size),
			 {ok, Data}
		 end,
    {GetSizeFun, PReadFun}.
