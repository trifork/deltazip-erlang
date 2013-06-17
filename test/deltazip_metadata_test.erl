-module(deltazip_metadata_test).

-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

one_rev_test() ->
    Bin0 = <<>>,
    Access0 = deltazip_util:bin_access(Bin0),
    DZa = deltazip:open(Access0),

    Rev1Data = <<"Hello">>,
    Rev1 = {Rev1Data, [{timestamp, erlang:universaltime()}]},
    AddSpec1 = deltazip:add(DZa, Rev1),
    Bin1 = deltazip_util:bin_replace_tail(Bin0, AddSpec1),
    deltazip:close(DZa),
    
    Access1 = deltazip_util:bin_access(Bin1),
    DZb = deltazip:open(Access1),
    ?assertEqual(Rev1Data, deltazip:get(DZb)),
    ?assertEqual({error, at_beginning}, deltazip:previous(DZb)),
    deltazip:close(DZb).
    

two_revs_test() ->
    Bin0 = <<>>,
    Access0 = deltazip_util:bin_access(Bin0),
    DZa = deltazip:open(Access0),

    %% Add Rev1.
    Rev1Data = <<"Hello">>,
    Rev1 = {Rev1Data, [{timestamp, erlang:universaltime()}]},
    AddSpec1 = deltazip:add(DZa, Rev1),
    Bin1 = deltazip_util:bin_replace_tail(Bin0, AddSpec1),
    deltazip:close(DZa),

    %% Check that Rev1 was added.
    Access1 = deltazip_util:bin_access(Bin1),
    DZb = deltazip:open(Access1),
    ?assertEqual(Rev1Data, deltazip:get(DZb)),
    ?assertEqual({error, at_beginning}, deltazip:previous(DZb)),

    %% Add Rev2.
    Rev2Data = <<"Hello, World!">>,
    Rev2 = {Rev2Data, [{timestamp,{{2000,1,1},{12,23,34}}},
                       {version_id, "AYBABTU"}]},
    AddSpec2 = deltazip:add(DZb, Rev2),
    Bin2 = deltazip_util:bin_replace_tail(Bin1, AddSpec2),
    deltazip:close(DZb),

    %% Check that Rev2 was added - and that both Rev1 and Rev2 can be accessed.
    Access2 = deltazip_util:bin_access(Bin2),
    DZc = deltazip:open(Access2),
    ?assertEqual(Rev1Data, deltazip:get(DZb)),
    {ok,DZc2} = deltazip:previous(DZc),
    ?assertEqual(Rev1Data, deltazip:get(DZb)),
    ?assertEqual({error, at_beginning}, deltazip:previous(DZb)),
    deltazip:close(DZc2).
