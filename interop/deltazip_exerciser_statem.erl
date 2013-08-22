-module(deltazip_exerciser_statem).

-export([initial_state/0,
         command/1,
         precondition/2,
         postcondition/3,
         next_state/3]).
-export([add_to_archive/3]).

-include_lib("triq/include/triq.hrl").
-include_lib("triq/include/triq_statem.hrl").

initial_state() ->
    [{<<>>, []}].

command(Archives) ->
    oneof([{call, ?MODULE, add_to_archive, [impl(), oneof(Archives), versions()]}]).

precondition(_State, _Cmd) -> true.

next_state(Archives, Var, {call, ?MODULE, add_to_archive, [Impl, OldArchive, NewVersions]}) ->
    try
        %% io:format(user, "DB| next_state: state=~p  var=~p\n", [Archives, Var]),
        {OldBin, OldVersions} = OldArchive,
        NewArchive = {Var, OldVersions ++ NewVersions},
        [NewArchive | Archives]
    catch Cls:Err ->
            Trace = erlang:get_stacktrace(),
            io:format(user, "DB| next_state failed: ~p\n** trace: ~p\n", [Err, Trace]),
            erlang:raise(Cls,Err,Trace)
    end.

postcondition(_State, {call, ?MODULE, add_to_archive, [_Impl, {OldArchive,OldVersions}, NewVersions]}, NewArchive) ->
    %% TODO: Verify with Java as well.
    EAllVersions = extract_all_versions_erlang(NewArchive),
    (EAllVersions == OldVersions++NewVersions).

%%%========== Implementation bridge: ========================================
add_to_archive(erlang, {OldBin,_}, NewVersions) ->
    try
        add_to_archive_erlang(OldBin, NewVersions)
    catch Cls:Err ->
            Trace = erlang:get_stacktrace(),
            io:format(user, "DB| add_to_archive failed: ~p\n** trace: ~p\n", [Err, Trace]),
            erlang:raise(Cls,Err,Trace)
    end.

%% TODO: Add Java.

add_to_archive_erlang(OldBin, NewVersions) ->
    Access = deltazip_util:bin_access(OldBin),
    DZ = deltazip:open(Access),
    try deltazip:add_multiple(DZ, NewVersions)
    after ok = deltazip:close(DZ)
    end.

extract_all_versions_erlang(Archive) ->
    DZ = deltazip:open(deltazip_util:bin_access(Archive)),
    {DZ2,Versions} = extract_all_versions_erlang(DZ, []),
    ok = deltazip:close(DZ),
    Versions.

extract_all_versions_erlang(DZ, Acc) ->
    case deltazip:get(DZ) of
        archive_is_empty when Acc==[] -> {DZ,Acc};
        {Data, Metadata} ->
            Acc2 = [{Data, Metadata} | Acc],
            case deltazip:previous(DZ) of
                {error, at_beginning} -> {DZ,Acc2};
                {ok, DZ2} -> extract_all_versions_erlang(DZ2, Acc2)
            end
    end.

%%%========== Generators: ========================================
impl() -> oneof([erlang]).

versions() -> list(version()).
version() -> {binary(), list(metadata_item())}.
metadata_item() -> oneof([{version_id, metadata_value()},
                          {ancestor, metadata_value()},
                          {timestamp, datetime()},
                          {numeric_keytag(), metadata_value()}]).
numeric_keytag() -> ?LET(X, pos_integer(), X+3).
metadata_value() -> binary().

datetime() ->
    {{choose(2000,2130), choose(1,12), choose(1,28)},
     {choose(0,23), choose(0,59), choose(0,59)}}.
