-module(deltazip_exerciser).

-compile(export_all).

-include_lib("triq/include/triq.hrl").
-include_lib("triq/include/triq_statem.hrl").

-define(STATEM_MODULE, deltazip_exerciser_statem).

run() ->
    regression1a(),
    regression1(),
    check_simple(400),
    check_statem(100),
    ok.

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

regression1() ->
    VersionA = <<49,199,38,89,216,93,112,61,199,3,33,217,135,56,57,
      98,223,218,230,136,115,31,1,102,215,59,78,207,232,
      209,162,235,1,38,89,216,216,93,112,61,199,3,33,217,
      135,219,141,9,59,220,91,250>>,
    VersionB = <<1,38,89,216,216,89,216,216,93,112,209,25,108,216,
      195,16,144,89,42,124,63,68,233,5,159,57,108,200,
      185,161,27,10,78,35,243,252,163,9,163,229,101,215,
      111,7,27,252,126,53,175,95,68,177,88,230,254,51,15,
      159,36,56,175,4,9,21,245,7,117,68,214,49,176,45,
      231,210,185,106,215,18,47,31,102,126,166,111,241,
      83,223,93,183,129,184,93,176,206,202,131,52,132>>,
    Versions = [{VersionA,[]}, {VersionB,[]}],
    EArchive = deltazip_proptest_util:add_to_archive_erlang(<<>>, Versions),
    JArchive = deltazip_proptest_util:add_to_archive_java(<<>>, Versions),
    true = deltazip_proptest_util:verify_archive(EArchive, Versions),
    true = deltazip_proptest_util:verify_archive(JArchive, Versions),
    true.


regression1a() ->
    %% More or less minimal version of regression1().
    VersionA = <<2,3,1,144,0,1,3,1,144,144,0,1>>,
    VersionB = <<0,1,144,144,0>>,
    Versions = [{VersionA,[]}, {VersionB,[]}],
    EArchive = deltazip_proptest_util:add_to_archive_erlang(<<>>, Versions),
    JArchive = deltazip_proptest_util:add_to_archive_java(<<>>, Versions),
    true = deltazip_proptest_util:verify_archive(EArchive, Versions),
    true = deltazip_proptest_util:verify_archive(JArchive, Versions),
    true.
