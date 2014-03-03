package org.fcrepo.federation.jcloudsconnector;


import org.modeshape.jcr.cache.DocumentStoreException;
import org.modeshape.jcr.value.BinaryValue;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.MutableBlobMetadata;
import org.jclouds.io.MutableContentMetadata;

/**
 * Utility methods for working with files in JClouds.
 * Reading function is in {@link JCloudsBinaryValue}.
 * Writing,updating,removing are here.
 */
public class JCloudsUtil {

	public static void write(String containerName, BlobStore blobStore,
			BinaryValue binary, String blobName) {

		try {

			Blob blob = blobStore.blobBuilder(blobName)
					.payload(binary.getStream()).build();

			MutableBlobMetadata bm = blob.getMetadata();

			MutableContentMetadata md = bm.getContentMetadata();

			// set metadata
			md.setContentLength(binary.getSize());
			// TODO "application/octet-stream" is default, but need get from
			// file type
			md.setContentType("application/octet-stream");

			blob.getMetadata().setContentMetadata(md);

			blobStore.putBlob(containerName, blob);

		} catch (Exception e) {
			throw new DocumentStoreException(containerName + "/" + blobName, e);
		}

	}

	public static void remove(String containerName, BlobStore blobStore,
			String blobName) {

		try {

			blobStore.removeBlob(containerName, blobName);

		} catch (Exception e) {
			throw new DocumentStoreException(containerName + "/" + blobName, e);
		}

	}

	public static boolean blobExists(String containerName, BlobStore blobStore,
			String blobName) {

		try {

			return (blobStore.blobExists(containerName, blobName) || blobStore
					.directoryExists(containerName, blobName)) ? true : false;

		} catch (Exception e) {
			throw new DocumentStoreException(containerName + "/" + blobName, e);
		}

	}

}
