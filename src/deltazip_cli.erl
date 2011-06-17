-module(deltazip_cli).

-export([main/1]).

main(L) ->
    interpret_command(L),
    init:stop().

interpret_command(["create", DZFile | InputFiles]) ->
    do_create(DZFile, InputFiles);
interpret_command(["get", DZFile]) ->
    do_get(DZFile).

do_create(DZFile, InputFiles) ->
    {ok, Fd} = file:open(DZFile, [write, exclusive, binary, raw]),
    Access = fd_access(Fd),
    DZ = deltazip:open(Access),
    Datas = [begin {ok,D} = file:read_file(F), D end
	     || F <- InputFiles],
    {0, Data} = deltazip:add_multiple(DZ, Datas),
    deltazip:close(DZ),
    ok = file:write(Fd, Data),
    ok = file:close(Fd).

do_get(DZFile) ->
    {ok, Fd} = file:open(DZFile, [read, binary, raw]),
    Access = fd_access(Fd),
    DZ = deltazip:open(Access),
    Data = deltazip:get(DZ),
    deltazip:close(DZ),
    ok = file:close(Fd),
    io:put_chars(Data). % TODO: Is non-ascii data handled correctly?

fd_access(Fd) ->
    GetSizeFun = fun() -> {ok, Pos} = file:position(Fd, eof), Pos end,
    PReadFun   = fun(Pos,Size) ->
			 {ok, Data} = file:pread(Fd, Pos, Size),
			 {ok, Data}
		 end,
    {GetSizeFun, PReadFun}.
