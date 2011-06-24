-module(deltazip_cli).

-export([main/1]).

main(L) ->
    put(exit_code, 0),
    Before = now(),
    interpret_command(L),
    After = now(),
    io:format(standard_error, "(Duration: ~.3fs)\n", [timer:now_diff(After, Before) * 1.0e-6]),
    init:stop(get(exit_code)).

interpret_command(["create", DZFile | InputFiles]) ->
    do_create(DZFile, InputFiles);
interpret_command(["add", DZFile | InputFiles]) ->
    do_add(DZFile, InputFiles);
interpret_command(["get", DZFile]) ->
    do_get(DZFile);
interpret_command(["get", "@" ++ NrStr, DZFile]) ->
    do_get(DZFile, list_to_integer(NrStr));
interpret_command(["count", DZFile]) ->
    do_count(DZFile);
interpret_command(["list", DZFile]) ->
    do_list(DZFile).

%%%======================================================================

%%%----------
do_create(DZFile, InputFiles) ->
    {ok, Fd} = file:open(DZFile, [write, exclusive, binary]),
    Access = fd_access(Fd),
    DZ = deltazip:open(Access),
    Datas = [begin {ok,D} = file:read_file(F), D end
	     || F <- InputFiles],
    {0, Data} = deltazip:add_multiple(DZ, Datas),
    deltazip:close(DZ),
    ok = file:write(Fd, Data),
    ok = file:close(Fd).

%%%----------
do_add(DZFile, InputFiles) ->
    {ok, Fd} = file:open(DZFile, [read, write, binary]),
    Access = fd_access(Fd),
    DZ = deltazip:open(Access),
    Datas = [begin {ok,D} = file:read_file(F), D end
	     || F <- InputFiles],
    {Pos, NewTail} = deltazip:add_multiple(DZ, Datas),
    deltazip:close(DZ),

    %% Write the new tail:
    ok = file:pwrite(Fd, Pos, NewTail),

    %% Truncate: (alas, there is no file:truncate/2)
    {ok,_} = file:position(Fd, Pos + iolist_size(NewTail)),
    ok = file:truncate(Fd),
    
    ok = file:close(Fd).

%%%----------
do_get(DZFile) ->
    do_get(DZFile, 0).

%%% Get version Nr counted from the most recent.
do_get(DZFile, Nr) ->
    {ok, Fd} = file:open(DZFile, [read, binary]),
    Access = fd_access(Fd),
    DZ = deltazip:open(Access),
    DZ2 = lists:foldl(fun(_,at_beginning) -> at_beginning;
			 (_,LocalDZ) ->
			      case deltazip:previous(LocalDZ) of
				  {ok, LocalDZ2} -> LocalDZ2;
				  {error,at_beginning} -> at_beginning
			      end
		      end,
		      DZ,
		      lists:seq(1,Nr)),
    if DZ2 == at_beginning ->
	    io:format(standard_error, "No version ~p exists\n", [Nr]),
	    put(exit_code, 1);
       true ->
	    Data = deltazip:get(DZ2),
	    deltazip:close(DZ2),
	    ok = file:close(Fd),
	    io:put_chars(Data) % TODO: Is non-ascii data handled correctly?
    end.

%%%----------
do_count(DZFile) ->
    {ok, Fd} = file:open(DZFile, [read, binary]),
    Access = fd_access(Fd),
    DZ = deltazip:open(Access),
    Cnt = count_entries(DZ),
    deltazip:close(DZ),
    ok = file:close(Fd),
    io:format("~b\n", [Cnt]).

count_entries(DZ) ->
    case deltazip:get(DZ) of
	file_is_empty -> 0;
	_ -> count_entries(DZ, 1)
    end.

count_entries(DZ, Acc) ->
    case deltazip:previous(DZ) of
	{ok, DZ2} -> count_entries(DZ2, Acc+1);
	{error, at_beginning} -> Acc
    end.

%%%----------
do_list(DZFile) ->
    {ok, Fd} = file:open(DZFile, [read, binary]),
    Access = fd_access(Fd),
    DZ = deltazip:open(Access),
    print_entry_stats(DZ),
    deltazip:close(DZ),
    ok = file:close(Fd).

print_entry_stats(DZ) ->
    io:format("~s:\t~s\t~s\t~s\t~s\n", ["Nr", "Method", "CompSize", "VersionSize", "Checksum"]),
    case deltazip:get(DZ) of
	file_is_empty -> 0;
	_ -> print_entry_stats(DZ, 0)
    end.
    
print_entry_stats(DZ, Nr) ->
    {Method,CompSize,Checksum} = deltazip:stats_for_current_entry(DZ),
    UncompSize = byte_size(deltazip:get(DZ)),
    io:format("~b:\t~2b\t~8b\t~8b\t~8.16b\n", [-Nr, Method, CompSize, UncompSize, Checksum]),
    case deltazip:previous(DZ) of
	{ok, DZ2} -> print_entry_stats(DZ2, Nr+1);
	{error, at_beginning} -> ok
    end.

%%%======================================================================

fd_access(Fd) ->
    GetSizeFun = fun() -> {ok, Pos} = file:position(Fd, eof), Pos end,
    PReadFun   = fun(Pos,Size) ->
			 {ok, Data} = file:pread(Fd, Pos, Size),
			 {ok, Data}
		 end,
    {GetSizeFun, PReadFun}.
