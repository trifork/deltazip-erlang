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
    ?LET({Archive={_,OldVersions},VersionSpecs}, {oneof(Archives), versions()},
         begin
             {VarSpecs, MDs} = lists:unzip(VersionSpecs),
             NewVersionData = further_versions_for(OldVersions, VarSpecs),
             NewVersions = lists:zip(NewVersionData, MDs),
             {call, ?MODULE, add_to_archive, [impl(), Archive, NewVersions]}
         end).

precondition(_State, _Cmd) -> true.

next_state(Archives, Var, {call, ?MODULE, add_to_archive, [_Impl, OldArchive, NewVersions]}) ->
    try
        %% io:format(user, "DB| next_state: state=~p  var=~p\n", [Archives, Var]),
        {_OldBin, OldVersions} = OldArchive,
        NewArchive = {Var, OldVersions ++ NewVersions},
        [NewArchive | Archives]
    catch Cls:Err ->
            Trace = erlang:get_stacktrace(),
            io:format(user, "DB| next_state failed: ~p\n** trace: ~p\n", [Err, Trace]),
            erlang:raise(Cls,Err,Trace)
    end.

postcondition(_State, {call, ?MODULE, add_to_archive, [_Impl, {_OldArchive,OldVersions}, NewVersions]}, NewArchive) ->
    ExpectedVersions = OldVersions++NewVersions,
    true == deltazip_proptest_util:verify_archive(NewArchive, ExpectedVersions).

add_to_archive(erlang, {OldBin,_}, NewVersions) ->
    try
        deltazip_proptest_util:add_to_archive_erlang(OldBin, NewVersions)
    catch Cls:Err ->
            Trace = erlang:get_stacktrace(),
            io:format(user, "DB| add_to_archive failed: ~p\n** trace: ~p\n", [Err, Trace]),
            erlang:raise(Cls,Err,Trace)
    end;
add_to_archive(java, {OldBin,_}, NewVersions) ->
    deltazip_proptest_util:add_to_archive_java(OldBin, NewVersions).

%%%========== Generators: ========================================
impl() -> oneof([erlang, java]).

versions() -> list(version()).
version() -> {deltazip_proptest_util:variation_spec(), list(metadata_item())}.
metadata_item() -> oneof([{version_id, metadata_value()},
                          {ancestor, metadata_value()},
                          {timestamp, datetime()},
                          {numeric_keytag(), metadata_value()}]).
numeric_keytag() -> ?LET(X, pos_integer(), X+3).
metadata_value() -> binary().

datetime() ->
    {{choose(2000,2130), choose(1,12), choose(1,28)},
     {choose(0,23), choose(0,59), choose(0,59)}}.

further_versions_for(OldVersions, VarSpecs) ->
    LastVersion = case OldVersions of
                      [] -> <<>>;
                      _ ->
                          {LastData,_MD} = lists:last(OldVersions),
                          LastData 
                  end,
    {NewVersions,_} =
        lists:mapfoldl(fun(VarSpec, PrevVersion) ->
                               V = deltazip_proptest_util:apply_variation_spec(VarSpec,PrevVersion),
                               {V,V}
                       end, LastVersion, VarSpecs),
    NewVersions.
