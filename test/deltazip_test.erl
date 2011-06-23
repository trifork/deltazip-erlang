-module(deltazip_test).

-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

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

    %% Check that Rev1 was added.
    Access1 = access(Bin1),
    DZb = deltazip:open(Access1),
    Rev1 = deltazip:get(DZb), % assertion
    {error, at_beginning} = deltazip:previous(DZb),

    %% Add Rev2.
    Rev2 = <<"Hello, World!">>,
    AddSpec2 = deltazip:add(DZb, Rev2),
    Bin2 = replace_tail(Bin1, AddSpec2),
    deltazip:close(DZb),
    
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

random_batch_test_() ->
    {timeout, 90,
     fun() ->
	     Batches = [[rnd_binary() || _ <- lists:seq(1,random:uniform(10))]
		     || _ <- lists:seq(1,20)],
	     test_batches(Batches, [], <<>>)
     end}.


random_patches_test_() ->
    {timeout, 300,
     fun() ->
	     {A,B,C} = now(), random:seed(A,B,C),
	     Revs = random_patches(200),
	     test_random(Revs, [], <<>>)
     end}.

random_patch_batch_test_() ->
    {timeout, 300,
     fun() ->
	     {A,B,C} = now(), random:seed(A,B,C),
	     Batches = batch_randomly(random_patches(200)),
	     test_batches(Batches, [], <<>>)
     end}.

random_variation_test_() ->
    {timeout, 300,
     fun() ->
	     {A,B,C} = now(), random:seed(A,B,C),
	     Revs = random_variations(200),
	     test_random(Revs, [], <<>>)
     end}.

random_variation_batch_test_() ->
    {timeout, 300,
     fun() ->
	     {A,B,C} = now(), random:seed(A,B,C),
	     Batches = batch_randomly(random_variations(200)),
	     test_batches(Batches, [], <<>>)
     end}.

%%%--------------------

batch_randomly([]) -> [];
batch_randomly(L) ->
    Len = length(L),
    BatchSz = random:uniform(min(Len, 10) + 1) - 1,
    {A,B} = lists:split(BatchSz, L),
    [A | batch_randomly(B)].


test_random([], _OldRevs, _Bin) ->
    ok;
test_random([Rev|Revs], OldRevs, Bin) ->
    io:format(user, "SZ| #~p: ~p -> ~p\n", [length(OldRevs), iolist_size(OldRevs), byte_size(Bin)]),
    Access = access(Bin),
    DZ = deltazip:open(Access),

    verify_history(DZ, OldRevs),

    %% Add Rev:
    AddSpec = deltazip:add(DZ, Rev),
    Bin2 = replace_tail(Bin, AddSpec),
    deltazip:close(DZ),
    test_random(Revs, [Rev|OldRevs], Bin2).


test_batches([], _OldRevs, _Bin) ->
    ok;
test_batches([Batch|Batches], OldRevs, Bin) ->
    io:format(user, "SZ| #~p: ~p -> ~p\n", [length(OldRevs), iolist_size(OldRevs), byte_size(Bin)]),
    Access = access(Bin),
    DZ = deltazip:open(Access),

    verify_history(DZ, OldRevs),

    %% Add Batch:
    AddSpec = deltazip:add_multiple(DZ, Batch),
    Bin2 = replace_tail(Bin, AddSpec),
    deltazip:close(DZ),
    test_batches(Batches, lists:reverse(Batch,OldRevs), Bin2).

verify_history(DZ, OldRevs) ->
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
    end.

random_patches(N) ->
    series(N, rnd_binary(), fun rnd_patch_binary/1).

random_variations(N) ->
    series(N, rnd_binary(), fun random_variation/1).

series(N, Start, Fun) ->
    {Revs, _Acc} =
	lists:mapfoldl(fun(_, Bin) ->
			       {Bin, Fun(Bin)}
		       end,
		       Start,
		       lists:seq(1,N)),
    Revs.
    

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
random_variation(Bin) ->
    Spec = variation_spec(byte_size(Bin)),
    iolist_to_binary(apply_variation_spec(Spec, Bin)).

variation_spec(Size) ->
    C = random:uniform(7),
    if 0<C, C=<2 -> same;
       2<C, C=<3 -> drop;
       3<C, C=<4 -> {replace, rnd_binary(2*Size + 100)};
       4<C, C=<7 ->
	    PreLen = random:uniform(Size+1)-1,
	    SufLen = random:uniform(Size+1)-1,
	    {split,
	     PreLen, variation_spec(PreLen),
	     SufLen, variation_spec(SufLen)}
    end.

apply_variation_spec(same, Bin) -> Bin;
apply_variation_spec(drop, _Bin) -> <<>>;
apply_variation_spec({replace, NewBin}, _Bin) -> NewBin;
apply_variation_spec({split, PreLen, PreSpec, SufLen, SufSpec}, Bin) ->
    <<Prefix:PreLen/binary, _/binary>> = Bin,
    NonSufLen =  byte_size(Bin) - SufLen,
    <<_:NonSufLen/binary, Suffix:SufLen/binary>> = Bin,
    [apply_variation_spec(PreSpec, Prefix) |
     apply_variation_spec(SufSpec, Suffix)].
    
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

