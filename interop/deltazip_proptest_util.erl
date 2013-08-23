-module(deltazip_proptest_util).

-export([add_to_archive_erlang/2, add_to_archive_java/2]).
-export([extract_all_versions_erlang/1, extract_all_versions_java/1]).
-export([verify_archive/2]).
%% -export([idl_encode_versions/1, idl_decode_versions/1]).
%% -export([idl_encode_binary/1, idl_decode_binary/1]).
-export([variation_spec/0, apply_variation_spec/2]).

-include_lib("triq/include/triq.hrl").
-include_lib("DeltaZipExerciser.hrl").

verify_archive(Archive, ExpectedVersions) ->
    EAllVersions = extract_all_versions_erlang(Archive),
    ErlangCheck = EAllVersions == ExpectedVersions,
    ErlangCheck orelse error_logger:error_msg("Discrepancy (E): Expected ~p,\n  got ~p\n", [ExpectedVersions, EAllVersions]),

    JAllVersions = (catch extract_all_versions_java(Archive)),
    JavaCheck = JAllVersions == ExpectedVersions,
    JavaCheck orelse error_logger:error_msg("Discrepancy (J): Expected ~p,\n  got ~p\n  on ~p\n", [ExpectedVersions, JAllVersions, Archive]),
    ErlangCheck and JavaCheck.

%%%========== Implementation bridge: ========================================

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

apply_variation_spec(Spec, Bin) ->
    iolist_to_binary(apply_variation_spec2(Spec,Bin)).

apply_variation_spec2(same, Bin) -> Bin;
apply_variation_spec2(drop, _Bin) -> <<>>;
apply_variation_spec2({replace, NewBin}, _Bin) -> NewBin;
apply_variation_spec2({split, PreRatio, PreSpec, SufRatio, SufSpec}, Bin) ->
    PreLen = trunc(PreRatio * byte_size(Bin)),
    SufLen = trunc(SufRatio * byte_size(Bin)),
    <<Prefix:PreLen/binary, _/binary>> = Bin,
    NonSufLen =  byte_size(Bin) - SufLen,
    <<_:NonSufLen/binary, Suffix:SufLen/binary>> = Bin,
    [apply_variation_spec2(PreSpec, Prefix) |
     apply_variation_spec2(SufSpec, Suffix)].
