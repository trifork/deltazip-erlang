-module(deltazip_test).

-include_lib("eunit/include/eunit.hrl").

empty_test() ->
    Bin0 = <<>>,
    Access0 = access(Bin0),

    DZa = deltazip:open(Access0),
    file_is_empty = deltazip:get(DZa),
    {error, at_beginning} = deltazip:previous(DZa),
    deltazip:close(DZa).

one_rev_test() ->
    Bin0 = <<>>,
    Access0 = access(Bin0),
    DZa = deltazip:open(Access0),

    Rev1 = <<"Hello">>,
    AddSpec1 = deltazip:add(DZa, Rev1),
    Bin1 = replace_tail(Bin0, AddSpec1),
    deltazip:close(DZa),
    
    Access1 = access(Bin1),
    DZb = deltazip:open(Access1),
    Rev1 = deltazip:get(DZb), % assertion
    {error, at_beginning} = deltazip:previous(DZb),
    deltazip:close(DZb).
    

two_revs_test() ->
    Bin0 = <<>>,
    Access0 = access(Bin0),
    DZa = deltazip:open(Access0),

    %% Add Rev1.
    Rev1 = <<"Hello">>,
    AddSpec1 = deltazip:add(DZa, Rev1),
    Bin1 = replace_tail(Bin0, AddSpec1),
    deltazip:close(DZa),

    io:format("DB| ~p:~p\n", [?MODULE, ?LINE]), 
    %% Check that Rev1 was added.
    Access1 = access(Bin1),
    DZb = deltazip:open(Access1),
    Rev1 = deltazip:get(DZb), % assertion
    {error, at_beginning} = deltazip:previous(DZb),

    io:format("DB| ~p:~p\n", [?MODULE, ?LINE]), 
    %% Add Rev2.
    Rev2 = <<"Hello, World!">>,
    AddSpec2 = deltazip:add(DZb, Rev2),
    Bin2 = replace_tail(Bin0, AddSpec2),
    deltazip:close(DZb),
    
    io:format("DB| Bin2 = ~p\n", [Bin2]), 
    io:format("DB| ~p:~p\n", [?MODULE, ?LINE]), 
    %% Check that Rev2 was added - and that both Rev1 and Rev2 can be accessed.
    Access2 = access(Bin2),
    DZc = deltazip:open(Access2),
    Rev2 = deltazip:get(DZc), % assertion
    {ok,DZc2} = deltazip:previous(DZc),
    Rev1 = deltazip:get(DZc2), % assertion
    {error, at_beginning} = deltazip:previous(DZc2),
    deltazip:close(DZc2).
    

random_test_() ->
    {timeout, 90,
     fun() ->
	     Revs = [rnd_binary() || _ <- lists:seq(1,100)],
	     test_random(Revs, [], <<>>)
     end}.


random_patches_test_() ->
    {timeout, 300,
     fun() ->
	     {A,B,C} = now(), random:seed(A,B,C),
	     Revs = random_patches(200),
	     test_random(Revs, [], <<>>)
     end}.

test_random([], _OldRevs, _Bin) ->
    ok;
test_random([Rev|Revs], OldRevs, Bin) ->
    io:format(user, "SZ| #~p: ~p -> ~p\n", [length(OldRevs), iolist_size(OldRevs), byte_size(Bin)]),
    Access = access(Bin),
    DZ = deltazip:open(Access),

    DZrewind = lists:foldl(fun(OldRev,LocalDZ) ->
				   Gotten = deltazip:get(LocalDZ),
				   {true,_} = {OldRev == Gotten, Gotten},
				   case deltazip:previous(LocalDZ) of
				       {ok, LocalDZ2} -> LocalDZ2;
				       {error, at_beginning} -> at_beginning
				   end
			   end,
			   DZ, OldRevs),
    case DZrewind of
	at_beginning -> ok;
	_ -> {error, at_beginning} = deltazip:previous(DZrewind)
    end,

    %% Add Rev:
    AddSpec = deltazip:add(DZ, Rev),
    Bin2 = replace_tail(Bin, AddSpec),
    deltazip:close(DZ),
    test_random(Revs, [Rev|OldRevs], Bin2).


random_patches(N) ->
    {Rnds, _Acc} =
	lists:mapfoldl(fun(_, Bin) ->
			       {Bin, rnd_patch_binary(Bin)}
		       end,
		       rnd_binary(),
		       lists:seq(1,N)),
    Rnds.

rnd_binary() ->
    Len = random:uniform(100000)-1,
    rnd_binary(Len).
rnd_binary(Len) ->
    L = [(random:uniform(256)-1) || _ <- lists:seq(1,Len)],
    list_to_binary(L).

rnd_patch_binary(OrgBin) ->
    iolist_to_binary(rnd_patch_binary_iol(OrgBin)).

rnd_patch_binary_iol(OrgBin) ->
    OrgSz = byte_size(OrgBin),
    C = random:uniform(100),
    if 0<C, C=<25, OrgSz>0 -> % Skip
	    Pos = random:uniform(OrgSz),
	    <<_:Pos/binary, Rest/binary>> = OrgBin,
	    rnd_patch_binary_iol(Rest);
       25<C, C=<75, OrgSz>0 -> % Copy
	    Len = random:uniform(min(OrgSz,70000)),
	    <<ToCopy:Len/binary, _/binary>> = OrgBin,
	    [ToCopy | rnd_patch_binary_iol(OrgBin)];
       75<C, C=<98 -> % Insert small
	    Len = random:uniform(100),
	    [rnd_binary(Len) | rnd_patch_binary_iol(OrgBin)];
       98<C, C=<100 -> % Insert large
	    Len = random:uniform(10000),
	    [rnd_binary(Len) | rnd_patch_binary_iol(OrgBin)];
       true -> []
    end.
	     

%%%--------------------

access(Bin) ->
    GetSizeFun = fun() -> byte_size(Bin) end,
    PReadFun   = fun(Pos,Size) ->
			 <<_:Pos/binary, Data:Size/binary, _/binary>> = Bin,
			 {ok, Data}
		 end,
    {GetSizeFun, PReadFun}.

	     
replace_tail(Bin, {PrefixLength, NewTail}) when is_integer(PrefixLength), (is_binary(NewTail) or is_list(NewTail)) ->
    NewTailBin = iolist_to_binary(NewTail),
    <<Prefix:PrefixLength/binary, _/binary>> = Bin,
    <<Prefix/binary, NewTailBin/binary>>.

