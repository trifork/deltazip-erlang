import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
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


    public DeltaZipExerciserPackage.Version[] extract_all_versions(String archive_in_b64)
        throws java.lang.Exception {
        byte[] archive = convertBinaryFromIDL(archive_in_b64);
        System.err.println("In extract_all_versions");

        DeltaZip dz = new DeltaZip(new DZUtil.ByteArrayAccess(archive));
        Version cur_version = dz.getVersion();
        ArrayList<Version> versions = new ArrayList<Version>();
        while (cur_version != null) {
            versions.add(cur_version);
            if (dz.hasPrevious()) {
                dz.previous();
                cur_version = dz.getVersion();
            } else break;
        }
        Collections.reverse(versions);
        return convertVersionsToIDL(versions);
    }

    private static byte[] convertBinaryFromIDL(String b64) {
        return javax.xml.bind.DatatypeConverter.parseBase64Binary(b64);
    }
    private static String convertBinaryToIDL(byte[] data) {
        return javax.xml.bind.DatatypeConverter.printBase64Binary(data);
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

    private static DeltaZipExerciserPackage.Version[] convertVersionsToIDL(List<Version> versions) {
        DeltaZipExerciserPackage.Version[] idl_versions = new DeltaZipExerciserPackage.Version[versions.size()];
        int v_pos=0;
        for (Version v : versions) {
            List<Metadata.Item> metadata = v.getMetadata();
            DeltaZipExerciserPackage.MetadataItem[] idl_metadata = new DeltaZipExerciserPackage.MetadataItem[metadata.size()];
            int m_pos = 0;
            for (Metadata.Item md : metadata) {
                DeltaZipExerciserPackage.MetadataItem idl_md = new DeltaZipExerciserPackage.MetadataItem();
                idl_md.keytag = md.getNumericKeytag();
                idl_md.value = convertBinaryToIDL(md.getValue());
                idl_metadata[m_pos++] = idl_md;
            }
            DeltaZipExerciserPackage.Version idl_v = new DeltaZipExerciserPackage.Version();
            idl_v.content = convertBinaryToIDL(DZUtil.allToByteArray(v.getContents()));
            idl_v.metadata = idl_metadata;
            idl_versions[v_pos++] = idl_v;
        }
        return idl_versions;
    }


    /** The request environment is exposed for error handling reasons. */
    public com.ericsson.otp.ic.Environment getEnv() {
        return _env;
    }
}
