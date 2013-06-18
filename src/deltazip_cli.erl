-module(deltazip_cli).

-export([main/1]).
-export([interpret_command/1]).

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
    do_list(DZFile);
interpret_command(["allow_ditto" | Rest]) ->
    put(allow_dittoflate, true), interpret_command(Rest);
interpret_command(["force_ditto" | Rest]) ->
    put(force_dittoflate, true), interpret_command(Rest);
interpret_command(["split", DZFile, Prefix]) ->
    do_split(DZFile, Prefix);
interpret_command(["rsplit", DZFile, Prefix]) ->
    do_rsplit(DZFile, Prefix);
interpret_command(["repack", OrgDZ, NewDZ]) ->
    do_repack(OrgDZ, NewDZ);
interpret_command(_) ->
    usage(),
    init:stop(1).

usage() ->
    Lines =
        ["Usage: deltazip <COMMAND> ARGS..."
         , "Commands:"
         , "  create <dzfile> <version-files> Create a deltazip archive"
         , "  add <dzfile> <version-files>    Add versions to an archive"
         , "  get  <dzfile>                   Print the last version"
         , "  get @n <dzfile>                 Print the nth-last version"
         , "  count <dzfile>                  Count the number of versions"
         , "  list <dzfile>                   List versions and their statistics"
         , "  repack <dzfile> <newdzfile>     Recompress archive"
         , "  split <dzfile> <file-prefix>    Split archive into files"
         , "  rsplit <dzfile> <file-prefix>   Split archive into files, numbered in reverse"
        ],
    [io:format("~s~n", [S]) || S <- Lines].

%%%======================================================================

%%%----------
do_create(DZFile, InputFiles) ->
    ItemSpecs = parse_file_list(InputFiles, [], true),
    Items = [begin {ok,D} = file:read_file(F), {D,MD} end
	     || {F,MD} <- ItemSpecs],
    do_create2(DZFile, Items).

parse_file_list([], _MDAcc, _AllowMetas) -> [];
parse_file_list(["--" | Rest], MDAcc, true) ->
    %% Stop recognizing metadata parameters.
    parse_file_list(Rest, MDAcc, false);
parse_file_list(["-m"++MetadataSpec | Rest], MDAcc, true=AllowMetas) ->
    MDItem = parse_metadata_spec(MetadataSpec),
    parse_file_list(Rest, [MDItem | MDAcc], AllowMetas);
parse_file_list([Filename | Rest], MDAcc, AllowMetas) ->
    [{Filename, lists:reverse(MDAcc)}
     | parse_file_list(Rest, [], AllowMetas)].

parse_metadata_spec(MetadataSpec) ->
    case re:run(MetadataSpec, "(.*?)=(.*)", [{capture, all_but_first, list}]) of
        {match, [KeyStr, ValueStr]} ->
            parse_specific_metadata(KeyStr, ValueStr);
        _ ->
            error({bad_metadata_specification, MetadataSpec})
    end.

parse_specific_metadata("timestamp", ValueStr) ->
    case ValueStr of
        "now" ->
            {timestamp, erlang:universaltime()};
        "+"++TSStr -> % Unix time input
            UnixTime = list_to_integer(TSStr),
            GregSecs = calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}}) + UnixTime,
            Datetime = calendar:gregorian_seconds_to_datetime(GregSecs),
            {timestamp, Datetime};
        TSStr ->
            case io_lib:fread("~4d-~2d-~2d ~2d:~2d:~2d", TSStr) of
                {ok, [Y,Mo,D, H,Mi,S],  []} ->
                    Datetime = {{Y,Mo,D}, {H,Mi,S}},
                    {timestamp, Datetime};
                _ ->
                    error({bad_timestamp_format, TSStr})
            end
    end;
parse_specific_metadata("version_id", ValueStr) ->
    {version_id, ValueStr};
parse_specific_metadata("ancestor", ValueStr) ->
    {ancestor, ValueStr};
parse_specific_metadata(Key, ValueStr) ->
    try list_to_integer(Key) of
        KeyTag ->
            {KeyTag, ValueStr}
    catch error:_ ->
            error({invalid_metadata_key, Key})
    end.


do_create2(DZFile, Datas) ->
    {ok, Fd} = file:open(DZFile, [write, exclusive, binary]),
    Access = deltazip_util:fd_access(Fd),
    DZ = deltazip:open(Access),
    {0, Data} = deltazip:add_multiple(DZ, Datas),
    deltazip:close(DZ),
    ok = file:write(Fd, Data),
    ok = file:close(Fd).

%%%----------
do_add(DZFile, InputFiles) ->
    {ok, Fd} = file:open(DZFile, [read, write, binary]),
    Access = deltazip_util:fd_access(Fd),
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
    Access = deltazip_util:fd_access(Fd),
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
	    {Data,_MD} = deltazip:get(DZ2),
	    deltazip:close(DZ2),
	    ok = file:close(Fd),
            %% TODO: Print metadata
	    io:format("~s", [Data]) % TODO: Is non-ascii data handled correctly?
    end.

%%%----------
do_count(DZFile) ->
    {ok, Fd} = file:open(DZFile, [read, binary]),
    Access = deltazip_util:fd_access(Fd),
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
    Access = deltazip_util:fd_access(Fd),
    DZ = deltazip:open(Access),
    print_entry_stats(DZ),
    deltazip:close(DZ),
    ok = file:close(Fd).

print_entry_stats(DZ) ->
    io:format("~s:\t~s\t~s\t~s\t~s\t~s\n",
              ["Nr", "Method", "CompSize", "VersionSize", "Checksum", "Metadata"]),
    case deltazip:get(DZ) of
	file_is_empty -> 0;
	_ -> print_entry_stats(DZ, 0)
    end.
    
print_entry_stats(DZ, Nr) ->
    {Method,CompSize,Checksum} = deltazip:stats_for_current_entry(DZ),
    UncompSize = byte_size(deltazip:get_data(DZ)),
    Metadata = deltazip:get_metadata(DZ),
    MetadataStr = stringify_metadata(Metadata),
    io:format("~b:\tM~b\t~8b\t~8b\t~8.16b\t~s\n",
              [-Nr, Method, CompSize, UncompSize, Checksum, MetadataStr]),
    case deltazip:previous(DZ) of
	{ok, DZ2} -> print_entry_stats(DZ2, Nr+1);
	{error, at_beginning} -> ok
    end.

stringify_metadata(Metadata) ->
    L = lists:map(fun({K,V}) ->
                          KStr = if is_integer(K) -> integer_to_list(K);
                                    is_atom(K)    -> atom_to_list(K)
                                 end,
                          VStr = stringify_metadata_value(K,V),
                          [KStr, "=\"", VStr, "\""]
                  end, Metadata),
    string:join(L, "; ").

stringify_metadata_value(timestamp, {{Y,Mo,D},{H,Mi,S}}) ->
    io_lib:format("~4b-~2..0b-~2..0b ~2..0b:~2..0b:~2..0b", [Y,Mo,D,H,Mi,S]);
stringify_metadata_value(_, Value) ->
    Value.


%%%----------
do_split(DZFile, Prefix) ->
    {ok, Fd} = file:open(DZFile, [read, binary]),
    Access = deltazip_util:fd_access(Fd),
    DZ = deltazip:open(Access),
    split_loop(DZ, Prefix, 1),
    deltazip:close(DZ),
    ok = file:close(Fd).


split_loop(DZ, Prefix, Nr) ->
    case deltazip:get_data(DZ) of
	file_is_empty -> 0;
	Data ->
	    FileName = lists:flatten(io_lib:format("~s~4..0b", [Prefix, Nr])),
	    ok = file:write_file(FileName, Data),
	    case deltazip:previous(DZ) of
		{ok, DZ2} -> split_loop(DZ2, Prefix, Nr+1);
		{error, at_beginning} -> ok
	    end		
    end.


do_rsplit(DZFile, Prefix) ->
    {ok, Fd} = file:open(DZFile, [read, binary]),
    Access = deltazip_util:fd_access(Fd),
    DZ = deltazip:open(Access),
    rsplit_loop(DZ, Prefix, 9999),
    deltazip:close(DZ),
    ok = file:close(Fd).


rsplit_loop(DZ, Prefix, Nr) ->
    case deltazip:get_data(DZ) of
	file_is_empty -> 0;
	Data ->
	    FileName = lists:flatten(io_lib:format("~s~4..0b", [Prefix, Nr])),
	    ok = file:write_file(FileName, Data),
	    case deltazip:previous(DZ) of
		{ok, DZ2} -> rsplit_loop(DZ2, Prefix, Nr-1);
		{error, at_beginning} -> ok
	    end		
    end.

%%%----------
do_repack(OrgDZ, NewDZ) ->
    {ok, Fd} = file:open(OrgDZ, [read, binary]),
    Access = deltazip_util:fd_access(Fd),
    DZ = deltazip:open(Access),
    Items = lists:reverse(read_loop(DZ)),
    deltazip:close(DZ),
    ok = file:close(Fd),
    do_create2(NewDZ, Items).

read_loop(DZ) ->
    case deltazip:get(DZ) of
	file_is_empty -> [];
	Item ->
	    [Item | case deltazip:previous(DZ) of
			{ok, DZ2} -> read_loop(DZ2);
			{error, at_beginning} -> []
		    end]
    end.

%%%======================================================================
