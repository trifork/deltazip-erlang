-module(deltazip_exerciser).

-compile(export_all).

-include_lib("triq/include/triq.hrl").
-include_lib("triq/include/triq_statem.hrl").

-define(STATEM_MODULE, deltazip_exerciser_statem).

run() ->
    check_statem(100).

check_statem(N) ->
    triq:check(prop_deltazip_statem(), N).

prop_deltazip_statem() ->
    ?FORALL(Cmds, commands(?STATEM_MODULE),
            begin
                {_,_,Res} = run_commands(?STATEM_MODULE, Cmds),
                Res==ok
            end).
