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

%% precondition(Archives, {add_to_archive, _Impl, {ID,_,_}, _Versions}) ->
%%     lists:keyfind(ID, 1, Archives) /= false;
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

postcondition(_State, {call, ?MODULE, add_to_archive, [_Impl, OldArchive, NewVersions]}, NewArchive) ->
    _AvoidWarnings = {OldArchive, NewVersions, NewArchive},
    true. % TODO!

%%%========== Implementation bridge: ========================================
add_to_archive(erlang, {OldBin,_}, NewVersions) ->
    %% io:format(user, "DB| add_to_archive: ~p\n", [[erlang, OldBin, NewVersions]]),
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
    NewBin = deltazip:add_multiple(DZ, NewVersions),
    ok = deltazip:close(DZ),
    NewBin.

%%%========== Generators: ========================================
impl() -> oneof([erlang]).

versions() -> list(version()).
version() -> {binary(), list(metadata_item())}.
metadata_item() -> {keytag(), metadata_value()}.
keytag() -> pos_integer().
metadata_value() -> binary().
