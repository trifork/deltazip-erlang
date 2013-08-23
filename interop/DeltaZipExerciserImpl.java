import java.util.ArrayList;
import java.nio.ByteBuffer;

import com.trifork.deltazip.DeltaZip;
import com.trifork.deltazip.Metadata;
import com.trifork.deltazip.Version;
import com.trifork.deltazip.DZUtil;

public class DeltaZipExerciserImpl extends _DeltaZipExerciserImplBase {
    public String add_to_archive(String archive_in_b64, DeltaZipExerciserPackage.Version[] versions)
        throws java.lang.Exception {
        byte[] archive = convertBinaryFromIDL(archive_in_b64);
        System.err.println("In add_to_archive");

        ArrayList<Version> dz_versions = convertVersionsFromIDL(versions);
        DZUtil.ByteArrayAccess access =
            new DZUtil.ByteArrayAccess(archive);
        DeltaZip dz = new DeltaZip(access);
        byte[] result = access.applyAppendSpec(dz.add(dz_versions.iterator()));
        System.err.println("add_to_archive: result length: "+result.length);

        return convertBinaryToIDL(result);
    }

    private static ArrayList<Version> convertVersionsFromIDL(DeltaZipExerciserPackage.Version[] idl_versions) {
        ArrayList<Version> dz_versions = new ArrayList<Version>();
        for (DeltaZipExerciserPackage.Version v : idl_versions) {
            ArrayList<Metadata.Item> metadata = new ArrayList<Metadata.Item>();
            for (DeltaZipExerciserPackage.MetadataItem md : v.metadata) {
                metadata.add(new Metadata.Item(md.keytag, convertBinaryFromIDL(md.value)));
            }
            byte[] content = convertBinaryFromIDL(v.content);
            dz_versions.add(new Version(ByteBuffer.wrap(content), metadata));
        }
        return dz_versions;
    }


    public DeltaZipExerciserPackage.Version[] extract_all_versions(String archive_in_base64)
        throws java.lang.Exception {
        System.err.println("In extract_all_versions");
        return null;
    }

    private static byte[] convertBinaryFromIDL(String b64) {
        return javax.xml.bind.DatatypeConverter.parseBase64Binary(b64);
    }
    private static String convertBinaryToIDL(byte[] data) {
        return javax.xml.bind.DatatypeConverter.printBase64Binary(data);
    }


    /** The request environment is exposed for error handling reasons. */
    public com.ericsson.otp.ic.Environment getEnv() {
        return _env;
    }
}
