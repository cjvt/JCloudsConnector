//package org.modeshape.connector.filesystem;
package org.fcrepo.federation.jcloudsconnector;

import java.io.InputStream;
import java.net.URL;
import javax.jcr.RepositoryException;
import org.modeshape.jcr.mimetype.MimeTypeDetector;
import org.modeshape.jcr.value.BinaryKey;
import org.modeshape.jcr.value.BinaryValue;
import org.modeshape.jcr.value.binary.UrlBinaryValue;
import org.jclouds.blobstore.BlobStore;

/**
 * A {@link BinaryValue} implementation used to read the content of a resolvable
 * URL. This class computes the {@link AbstractBinary#getMimeType() MIME type}
 * lazily.
 */
public class JCloudsBinaryValue extends UrlBinaryValue {//ExternalBinaryValue {
	private static final long serialVersionUID = 1L;

	private URL url;
	
	private BlobStore blobStore;
	private String containerName;
	private String blobName ;

	public JCloudsBinaryValue(BinaryKey key, String sourceName, URL content,
			BlobStore blobStore,String containerName, String blobName, 
			long size, String nameHint, MimeTypeDetector mimeTypeDetector) {
		super(key, sourceName, content, size, nameHint,	mimeTypeDetector);

		this.url = content;//TODO
		
		this.blobStore =blobStore;
		this.containerName=containerName;
		this.blobName =blobName;

	}

	protected URL toUrl() {
		return url;
	}

	@Override
	public InputStream getStream() throws RepositoryException {
		try {
			if(!blobStore.blobExists(containerName, blobName)){
				throw new RepositoryException("Blob " + blobName + " does not exsit in container " + containerName);
				
			}
			
			return blobStore.getBlob(containerName, blobName).getPayload().getInput();

		} catch (Exception e) {
			throw new RepositoryException(e);
		}
	}
}
