-module(deltazip_exerciser_statem).

-export([initial_state/0,
         command/1,
         precondition/2,
         postcondition/3,
         next_state/3]).
-export([add_to_archive/3]).

-include_lib("triq/include/triq.hrl").
-include_lib("triq/include/triq_statem.hrl").

-include_lib("DeltaZipExerciser.hrl").

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
    %% TODO: Verify with Java as well.
    ExpectedVersions = OldVersions++NewVersions,

    EAllVersions = extract_all_versions_erlang(NewArchive),
    ErlangCheck = EAllVersions == ExpectedVersions,
    ErlangCheck orelse error_logger:error_msg("Discrepancy (E): Expected ~p,\n  got ~p\n", [ExpectedVersions, EAllVersions]),

    JAllVersions = extract_all_versions_java(NewArchive),
    JavaCheck = JAllVersions == ExpectedVersions,
    JavaCheck orelse error_logger:error_msg("Discrepancy (J): Expected ~p,\n  got ~p\n", [ExpectedVersions, JAllVersions]),
    ErlangCheck and JavaCheck.

%%%========== Implementation bridge: ========================================
add_to_archive(erlang, {OldBin,_}, NewVersions) ->
    try
        add_to_archive_erlang(OldBin, NewVersions)
    catch Cls:Err ->
            Trace = erlang:get_stacktrace(),
            io:format(user, "DB| add_to_archive failed: ~p\n** trace: ~p\n", [Err, Trace]),
            erlang:raise(Cls,Err,Trace)
    end;
add_to_archive(java, {OldBin,_}, NewVersions) ->
    add_to_archive_java(OldBin, NewVersions).

add_to_archive_erlang(OldBin, NewVersions) ->
    Access = deltazip_util:bin_access(OldBin),
    DZ = deltazip:open(Access),
    try deltazip:add_multiple(DZ, NewVersions)
    after ok = deltazip:close(DZ)
    end.

add_to_archive_java(OldBin, NewVersions) ->
    {ok,Host}=inet:gethostname(),
    JavaServer = {dummy, list_to_atom("deltazip_java@"++Host)},

    %% At present, the Java IDL bindings won't know of binaries :-/
    NewVersionsEnc = idl_encode_versions(NewVersions),
    OldBinEnc = idl_encode_binary(OldBin),
    Request = {add_to_archive, OldBinEnc, NewVersionsEnc},

    %% io:format("DB| Request to Java: ~p\n", [Request]),
    case gen_server:call(JavaServer, Request) of
        {error, Err} -> error({java_side_error, Err});
        NewBinFromIDL -> idl_decode_binary(NewBinFromIDL)
    end.

extract_all_versions_erlang(Archive) ->
    DZ = deltazip:open(deltazip_util:bin_access(Archive)),
    {DZ2,Versions} = extract_all_versions_erlang(DZ, []),
    ok = deltazip:close(DZ2),
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

extract_all_versions_java(Archive) ->
    {ok,Host}=inet:gethostname(),
    JavaServer = {dummy, list_to_atom("deltazip_java@"++Host)},

    %% At present, the Java IDL bindings won't know of binaries :-/
    ArchiveEnc = idl_encode_binary(Archive),
    Request = {extract_all_versions, ArchiveEnc},

    %% io:format("DB| Request to Java: ~p\n", [Request]),
    case gen_server:call(JavaServer, Request) of
        {error, Err} -> error({java_side_error, Err});
        AllVersionsIDL -> idl_decode_versions(AllVersionsIDL)
    end.

%%%========== Conversions to and from IDL: ===================================

idl_encode_versions(Versions) ->
    [#'DeltaZipExerciser_Version'{
        content=idl_encode_binary(D),
        metadata=[idl_encode_metadata_item(MD) || MD <- MDs]
       }
     || {D,MDs} <- Versions].

idl_encode_metadata_item(MDItem) ->
    {Keytag, Value} = deltazip_metadata:encode_symbolic(MDItem),
    #'DeltaZipExerciser_MetadataItem'{
                          keytag=Keytag,
                          value=idl_encode_binary(Value)}.
idl_decode_versions(Versions) ->
    [{idl_decode_binary(D), [idl_decode_metadata_item(MD) || MD <- MDs]}
     || #'DeltaZipExerciser_Version'{content=D, metadata=MDs} <- Versions].

idl_decode_metadata_item(#'DeltaZipExerciser_MetadataItem'{
                            keytag=Keytag,
                            value=Value}) ->
    
    deltazip_metadata:decode_symbolic({Keytag, idl_decode_binary(Value)}).

idl_encode_binary(X) ->
    binary_to_list(base64:encode(X)).
idl_decode_binary(X) ->
    base64:decode(list_to_binary(X)).

%%%========== Generators: ========================================
impl() -> oneof([erlang, java]).

versions() -> list(version()).
version() -> {variation_spec(), list(metadata_item())}.
metadata_item() -> oneof([{version_id, metadata_value()},
                          {ancestor, metadata_value()},
                          {timestamp, datetime()},
                          {numeric_keytag(), metadata_value()}]).
numeric_keytag() -> ?LET(X, pos_integer(), X+3).
metadata_value() -> binary().

datetime() ->
    {{choose(2000,2130), choose(1,12), choose(1,28)},
     {choose(0,23), choose(0,59), choose(0,59)}}.

variation_spec() ->
    frequency([{2, same},
               {1, drop},
               {1, {replace, binary()}},
               {3, {split,
                    real_0_1(), ?DELAY(variation_spec()),
                    real_0_1(), ?DELAY(variation_spec())}}
              ]).

real_0_1() ->
    ?LET(X, choose(0,1 bsl 32), X / (1 bsl 32)).

apply_variation_spec(same, Bin) -> Bin;
apply_variation_spec(drop, _Bin) -> <<>>;
apply_variation_spec({replace, NewBin}, _Bin) -> NewBin;
apply_variation_spec({split, PreRatio, PreSpec, SufRatio, SufSpec}, Bin) ->
    PreLen = trunc(PreRatio * byte_size(Bin)),
    SufLen = trunc(SufRatio * byte_size(Bin)),
    <<Prefix:PreLen/binary, _/binary>> = Bin,
    NonSufLen =  byte_size(Bin) - SufLen,
    <<_:NonSufLen/binary, Suffix:SufLen/binary>> = Bin,
    [apply_variation_spec(PreSpec, Prefix) |
     apply_variation_spec(SufSpec, Suffix)].

further_versions_for(OldVersions, VarSpecs) ->
    LastVersion = case OldVersions of
                      [] -> <<>>;
                      _ ->
                          {LastData,_MD} = lists:last(OldVersions),
                          LastData 
                  end,
    {NewVersions,_} =
        lists:mapfoldl(fun(VarSpec, PrevVersion) ->
                               V = iolist_to_binary(apply_variation_spec(VarSpec,PrevVersion)),
                               {V,V}
                       end, LastVersion, VarSpecs),
    NewVersions.
