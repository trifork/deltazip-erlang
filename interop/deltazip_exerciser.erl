-module(deltazip_exerciser).

-compile(export_all).

-include_lib("triq/include/triq.hrl").
-include_lib("triq/include/triq_statem.hrl").

-define(STATEM_MODULE, deltazip_exerciser_statem).

run() ->
    check_simple(400),
    check_statem(100).

check_simple(N) ->
    triq:check(prop_deltazip_ab(), N).

check_statem(N) ->
    triq:check(prop_deltazip_statem(), N).

prop_deltazip_statem() ->
    ?FORALL(Cmds, commands(?STATEM_MODULE),
            begin
                {_,_,Res} = run_commands(?STATEM_MODULE, Cmds),
                Res==ok
            end).

prop_deltazip_ab() ->
    ?FORALL({VersionA, VarSpec},
            {binary(), deltazip_proptest_util:variation_spec()},
            begin
                VersionB = deltazip_proptest_util:apply_variation_spec(VarSpec,VersionA),
                Versions = [{VersionA,[]}, {VersionB,[]}],
                EArchive = deltazip_proptest_util:add_to_archive_erlang(<<>>, Versions),
                JArchive = deltazip_proptest_util:add_to_archive_java(<<>>, Versions),
                true = deltazip_proptest_util:verify_archive(EArchive, Versions),
                true = deltazip_proptest_util:verify_archive(JArchive, Versions),
                true
            end).
