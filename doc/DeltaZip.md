<head>
<meta HTTP-EQUIV="content-type" CONTENT="text/html; charset=UTF-8">
<style>
  pre {margin: 0em 4em;}
  table {border: thin solid black;}
  table td {border: thin solid black;}
/**/
  table.format td {text-align: center; padding: 0em 1em; }
  table.enum {border-collapse: collapse;}
  table.enum th {padding: 0em 1em; text-align: left;}
  table.enum td {padding: 0.1em 1em; border-width: thin 0px;}
  table.enum td:first-child {text-align: right; padding: 0em 4em;}
</style>
</head>

# DeltaZip

DeltaZip is a compact file format for storing a sequence of versions of a file.

By taking advantage of the redundancy in the version history, the sequence can be stored very compactly, compared to storing each version separately.

## File format overview

A DeltaZip file (or DeltaZip archive) consists of a sequence of *chapters*, each containing one version of the file. The last chapter contains the latest version, using normal compression - it is a *self-contained* chapter. In the other, previous chapters, the corresponding version is stored, expressed as the *difference* between that version and the following - they are *delta-chapters*.
(In other words, what is stored in a delta-chapter is a description of how to construct the version corresponding to the chapter, given the next version in the sequence.)

<table class="format" style="border: thin solid black">
<tr>
<th>Ch. 1</th><th>Ch. 2</th><th>...</th><th>Ch. (N-1)</th><th>Ch. N</th>
</tr>

<tr>
<td>V<sub>1</sub> &#8722; V<sub>2</sub></td>
<td>V<sub>2</sub> &#8722; V<sub>3</sub></td>
<td>&nbsp;&nbsp;&hellip;&nbsp;&nbsp;</td>
<td>V<sub>N-1</sub> &#8722; V<sub>N</sub></td>
<td>V<sub>N</sub></td>
</tr>
</table>

## Properties of the DeltaZip format
Using the DeltaZip format, the following can be achieved:
  - compact representation
  - easy access to the latest version
  - easy addition of a newer version
  - relatively quick access to older versions
  - easy deletion of the oldest versions.

### Compact representation
The size of the delta representation may be a few per mille of the full versions, depending on the nature of the changes.
Version histories of a few hundred versions may thus be stored in a file having the same order of size as the latest version in itself.

### Easy access to the latest version
The latest chapter is easily locatable, and contains the latest version compressed using normal compression algorithms, and it can therefore easily be retrieved.

### Relatively quick access to older versions
Retrieval of a version which is not the newest is done by going backwards from the most recent version, reconstructing each of the intermediate versions in turn, until the desired version is reached.

The Erlang implementation of DeltaZip has demonstrated an unpacking speed of ~250MB/s (on 2011 laptop hardware).

### Easy deletion of oldest versions
Truncating the history by deleting a number of the oldest versions is done by simply removing the corresponding number of chapters from the beginning of the archive. Because a DeltaZip archive is always accessed backwards, from the latest chapters towards the earliest, the remaining versions can be accessed just as before the deletion.

## Technical limits
The file format in the present form only supports (compressed) version sizes up to 2<sup>27</sup>-1 bytes, or 128MB.

## File format details

### Common types

    varlen_int ::= {0:1, Bits:7}            // value is Bits
                 | {1:1, Bits:7} varlen_int // Bits are MSB of the value.

    varlen_string ::= (Length:varlen_int) (Value:byte[Length])

### Gross structure

    archive ::= header chapter*
    header  ::= deltazip-magic-number
    deltazip-magic-number ::= 0xCE 0xB4 0x7A version
    version ::= {major:4, minor:4}

The defined versions are 1.0 and 1.1.

- 1.0: Original version
- 1.1: Added metadata-support. (Max. chapter size reduced from 256MB to 128MB.)

### Chapters
    chapter     ::= chapter-tag adler metadata data chapter-tag
    adler       ::= uint32
    data        ::= byte* // Length is (Size - |metadata|)

*For version 1.0:*

    chapter-tag ::= {Method:4, Size:28}
    metadata    ::= void  // Metadata not supported in 1.0

*For version 1.1:*

    chapter-tag ::= {Method:4, Metas:1, Size:27}
    metadata    : See the "Chapter metadata" section.
    metadata_item ::= (Tag:varlen_int) (MDSize:varlen_int) (MD:byte[MDSize])


The leading and trailing chapter-tag must be identical.

'Adler' is the Adler32 checksum of the (raw, uncompressed) version
contained in the chapter.

The metadata section is a key-value dictionary with integer keys. It
is described below.

The Method is a number signifying how the chapter's file version is represented.

The values 0-3 are used for self-contained chapters:

<table class="enum">
<tr><th>Method value</th><th>Method name</th></tr>
<tr><td>0</td><td>Uncompressed (raw).</td></tr>
<tr><td>1</td><td>Deflated (using window size 32K, no zlib header).</td></tr>
<tr><td>2&ndash;3</td><td>Unassigned.</td></tr>
</table>

The values 4-15 are used for delta-chapters:
<table class="enum">
<tr><th>Method value</th><th>Method name</th></tr>
<tr><td>4</td><td>Chunked.</td></tr>
<tr><td>5</td><td>Chunked-Middle.</td></tr>
<tr><td>6</td><td>Reserved for Dittoflate.</td></tr>
<tr><td>7</td><td>Chunked-Middle2.</td></tr>
<tr><td>8&ndash;15</td><td>Unassigned.</td></tr>
</table>

The delta compression methods are described below.

### Chapter metadata

The metadata section is a key-value dictionary with integer keys and
arbitrary byte-string values.

    metadata    ::= (MetadataSize:varlen_int) metadata_item* metadata_cksum
    metadata_item ::= metadata_keytag metadata_value
    metadata_keytag ::= varlen_int
    metadata_value ::= varlen_string
    metadata_cksum ::= byte

MetadataSize is the total size of the metadata_items.
The total length of the metadata section is therefore
1 + MetadataSize + (the size of MetadataSize itself).

metadata_cksum is a rudimentary checksum; it is chosen such that the sum of
all bytes in metadata, modulo 255, is zero. It is calculated as the
two's complement of the modular sum of the rest of the bytes of
metadata.

#### Metadata tags
The defined tag values are as follows:

<table class="enum">
<tr><th>Key tag</th><th>Meaning</th><th>Representation</th></tr>
<tr><td>1</td><td>Timestamp</td><td>
        Seconds since New Year 2000, represented as a
        uint32 (in network order).
</td></tr>
<tr><td>2</td><td>Version identifier</td><td>Any byte-string.</td></tr>
<tr><td>3</td><td>Ancestor</td><td>
        A version identifier. Used to describe non-linear version histories.
</td></tr>
</table>


### Delta compression methods

The notation "array[a;b]" means the sub-array which is obtained by
taking the "b" first elements of "array", then removing the first "a"
elements.

#### The "Chunked" compression method:

    chunked-chapter-data ::= chunk*
    chunk        ::= chunk-header chunk-size chunk-data
    chunk-header ::= {ChunkMethod:5 ChunkParams:3}
    chunk-size   ::= uint16
    chunk-data   ::= byte* // Length specified by chunk-size

Chunk methods:

<table class="enum">
<tr><th>Chunk method value</th><th>Method name</th></tr>
<tr><td>0</td><td>Deflated.</td></tr>
<tr><td>1</td><td>Prefix-copy.</td></tr>
<tr><td>2</td><td>Offset-copy.</td></tr>
<tr><td>3&ndash;31</td><td>Unassigned.</td></tr>
</table>

Chunk methods representations:

    chunk-data ::= deflate-chunk-data     // when ChunkMethod=0
    chunk-data ::= prefix-copy-chunk-data // when ChunkMethod=1
    chunk-data ::= offset-copy-chunk-data // when ChunkMethod=2

    deflate-chunk-data ::= byte*
    prefix-copy-chunk-data ::= copy-lengthM1
    offset-copy-chunk-data ::= offset-lengthM1 copy-lengthM1

    // Lengths minus one:
    copy-lengthM1   ::= uint16
    offset-lengthM1 ::= uint16

Algorithm for "Chunked":

    WINDOW_SIZE := 0x7E00;
    CHUNK_SIZE  := WINDOW_SIZE / 2;
    RSKIP_GRANULARITY := CHUNK_SIZE / 2;

    chunked() {
        org_pos := 0;
        output := empty;
        for each chunk c : apply chunk c
    }

    apply deflate-chunk-data(params, chunk_data) {
        rskip := params * RSKIP_GRANULARITY;
        org_pos := org_pos + rskip;
        dict := org[org_pos; min(org_pos + WINDOW_SIZE, |org|)]
        output := output ++ inflate_with_dict(chunk_data, dict, window_size:32, zlib_headers:off)
    }

    apply prefix-copy-chunk-data(copy_lengthM1) {
        copy_lengthM1 = copy_length+1
        output := output ++ org[org_pos; org_pos + copy_length]
        org_pos := org_pos + copy_length;
    }

    apply prefix-copy-chunk-data(offset_lengthM1, copy_lengthM1) {
        offset_lengthM1 = offset_length+1
        org_pos := org_pos + offset_length;
        apply prefix-copy-chunk-data(copy_lengthM1);
    }


#### The "Chunked Middle" compression method:

The old and new version have a common prefix and a common suffix.
The middle of the old version is chunk-encoded relative to the middle of the new version.


Representation:

    chunked-middle-chapter-data ::= prefix-length suffix-length chunked-chapter-data
    prefix-length ::= varlen_int
    suffix-length ::= varlen_int

Algorithm for "Chunked Middle":

    chunked_middle(prefix_length, suffix_length) {
        org := org[prefix_length; |org| - suffix_length];
        chunked()
    }


#### The "Chunked Middle 2" compression method:

The old and new version have a common prefix and a common suffix.
The middle of the old version is chunk-encoded relative to a part of
the new version starting up to WINDOW_SIZE/2 before the prefix ends.

Representation:

    chunked-middle2-chapter-data ::= prefix-length suffix-length chunked-chapter-data
    prefix-length ::= varlen_int
    suffix-length ::= varlen_int

Algorithm for "Chunked Middle 2":

    chunked_middle(prefix_length, suffix_length) {
        cut_length := max(0, prefix_length - WINDOW_SIZE/2);
        org := org[cut_length; |org|];
        chunked()
    }
