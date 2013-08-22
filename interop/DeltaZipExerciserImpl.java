public class DeltaZipExerciserImpl extends _DeltaZipExerciserImplBase {
    public byte[] add_to_archive(byte[] archive, DeltaZipExerciserPackage.Version[] versions)
        throws java.lang.Exception {
        System.err.println("In add_to_archive\n");
        return null;
    }


    public DeltaZipExerciserPackage.Version[] extract_all_versions(byte[] archive)
        throws java.lang.Exception {
        System.err.println("In add_to_archive\n");
        return null;
    }

    /** The request environment is exposed for error handling reasons. */
    public com.ericsson.otp.ic.Environment getEnv() {
        return _env;
    }
}
