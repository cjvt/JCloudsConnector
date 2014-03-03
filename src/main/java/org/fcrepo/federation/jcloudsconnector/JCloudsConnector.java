/*
 * ModeShape (http://www.modeshape.org)
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * See the AUTHORS.txt file in the distribution for a full listing of
 * individual contributors.
 *
 * ModeShape is free software. Unless otherwise indicated, all code in ModeShape
 * is licensed to you under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * ModeShape is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
//package org.modeshape.connector.filesystem;
package org.fcrepo.federation.jcloudsconnector;

import java.io.File;

import java.io.IOException;

import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import javax.jcr.NamespaceRegistry;
import javax.jcr.RepositoryException;
import org.infinispan.schematic.document.Document;
import org.modeshape.common.util.StringUtil;
import org.modeshape.jcr.JcrI18n;
import org.modeshape.jcr.JcrLexicon;
import org.modeshape.jcr.RepositoryConfiguration;
import org.modeshape.jcr.api.nodetype.NodeTypeManager;
import org.modeshape.jcr.cache.DocumentStoreException;
import org.modeshape.jcr.federation.spi.Connector;
import org.modeshape.jcr.federation.spi.DocumentChanges;
import org.modeshape.jcr.federation.spi.DocumentReader;
import org.modeshape.jcr.federation.spi.DocumentWriter;
import org.modeshape.jcr.federation.spi.PageKey;
import org.modeshape.jcr.federation.spi.Pageable;
import org.modeshape.jcr.federation.spi.WritableConnector;
import org.modeshape.jcr.value.BinaryKey;
import org.modeshape.jcr.value.BinaryValue;
import org.modeshape.jcr.value.Name;
import org.modeshape.jcr.value.Property;
import org.modeshape.jcr.value.binary.ExternalBinaryValue;
import org.modeshape.jcr.value.binary.UrlBinaryValue;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.ListContainerOptions;

/**
 * {@link Connector} implementation that exposes a single directory on the local
 * file system. This connector has several properties that must be configured
 * via the {@link RepositoryConfiguration}:
 * <ul>
 * <li><strong><code>directoryPath</code></strong> - The path to the file or
 * folder that is to be accessed by this connector.</li>
 * <li><strong><code>readOnly</code></strong> - A boolean flag that specifies
 * whether this source can create/modify/remove files and directories on the
 * file system to reflect changes in the JCR content. By default, sources are
 * not read-only.</li>
 * <li><strong><code>addMimeTypeMixin</code></strong> - A boolean flag that
 * specifies whether this connector should add the 'mix:mimeType' mixin to the
 * 'nt:resource' nodes to include the 'jcr:mimeType' property. If set to
 * <code>true</code>, the MIME type is computed immediately when the
 * 'nt:resource' node is accessed, which might be expensive for larger files.
 * This is <code>false</code> by default.</li>
 * <li><strong><code>extraPropertyStorage</code></strong> - An optional string
 * flag that specifies how this source handles "extra" properties that are not
 * stored via file system attributes. See {@link #extraPropertiesStorage} for
 * details. By default, extra properties are stored in the same Infinispan cache
 * that the repository uses.</li>
 * <li><strong><code>exclusionPattern</code></strong> - Optional property that
 * specifies a regular expression that is used to determine which files and
 * folders in the underlying file system are not exposed through this connector.
 * Files and folders with a name that matches the provided regular expression
 * will <i>not</i> be exposed by this source.</li>
 * <li><strong><code>inclusionPattern</code></strong> - Optional property that
 * specifies a regular expression that is used to determine which files and
 * folders in the underlying file system are exposed through this connector.
 * Files and folders with a name that matches the provided regular expression
 * will be exposed by this source.</li>
 * </ul>
 * Inclusion and exclusion patterns can be used separately or in combination.
 * For example, consider these cases:
 * <table cellspacing="0" cellpadding="1" border="1">
 * <tr>
 * <th>Inclusion Pattern</th>
 * <th>Exclusion Pattern</th>
 * <th>Examples</th>
 * </tr>
 * <tr>
 * <td>(.+)\\.txt$</td>
 * <td></td>
 * <td>Includes only files and directories whose names end in "<code>.txt</code>
 * " (e.g., "<code>something.txt</code>" ), but does not include files and other
 * folders such as "<code>something.jar</code>" or "
 * <code>something.txt.zip</code>".</td>
 * </tr>
 * <tr>
 * <td>(.+)\\.txt$</td>
 * <td>my.txt</td>
 * <td>Includes only files and directories whose names end in "<code>.txt</code>
 * " (e.g., "<code>something.txt</code>" ) with the exception of "
 * <code>my.txt</code>", and does not include files and other folders such as "
 * <code>something.jar</code>" or " <code>something.txt.zip</code>".</td>
 * </tr>
 * <tr>
 * <td>my.txt</td>
 * <td>.+</td>
 * <td>Excludes all files and directories except any named "<code>my.txt</code>
 * ".</td>
 * </tr>
 * </table>
 */
public class JCloudsConnector extends WritableConnector implements Pageable {

	private static final String FILE_SEPARATOR = System
			.getProperty("file.separator");
	private static final String DELIMITER = "/";
	private static final String NT_FOLDER = "nt:folder";
	private static final String NT_FILE = "nt:file";
	private static final String NT_RESOURCE = "nt:resource";
	private static final String MIX_MIME_TYPE = "mix:mimeType";
	private static final String JCR_PRIMARY_TYPE = "jcr:primaryType";
	private static final String JCR_DATA = "jcr:data";
	private static final String JCR_MIME_TYPE = "jcr:mimeType";
	private static final String JCR_ENCODING = "jcr:encoding";
	private static final String JCR_CREATED = "jcr:created";
	private static final String JCR_CREATED_BY = "jcr:createdBy";
	private static final String JCR_LAST_MODIFIED = "jcr:lastModified";
	private static final String JCR_LAST_MODIFIED_BY = "jcr:lastModified";
	private static final String JCR_CONTENT = "jcr:content";
	private static final String JCR_CONTENT_SUFFIX = DELIMITER + JCR_CONTENT;
	private static final int JCR_CONTENT_SUFFIX_LENGTH = JCR_CONTENT_SUFFIX
			.length();

	private BlobStoreContext ctx;
	private BlobStore blobStore;
	private String identity;
	private String credential;
	private String containerName;
	private String provider;// = "aws-s3";
	private String providerUrlPrefix; // "https://s3.amazonaws.com"

	/**
	 * The string path for a {@link File} object that represents the top-level
	 * directory accessed by this connector. This is set via reflection and is
	 * required for this connector.
	 */
	private String directoryPath;
	private File directory;

	/**
	 * A string that is created in the
	 * {@link #initialize(NamespaceRegistry, NodeTypeManager)} method that
	 * represents the absolute path to the {@link #directory}. This path is
	 * removed from an absolute path of a file to obtain the ID of the node.
	 */
	private String directoryAbsolutePath;
	private int directoryAbsolutePathLength;

	/**
	 * A boolean flag that specifies whether this connector should add the
	 * 'mix:mimeType' mixin to the 'nt:resource' nodes to include the
	 * 'jcr:mimeType' property. If set to <code>true</code>, the MIME type is
	 * computed immediately when the 'nt:resource' node is accessed, which might
	 * be expensive for larger files. This is <code>false</code> by default.
	 */
	private boolean addMimeTypeMixin = false;

	/**
	 * The maximum number of children a folder will expose at any given time.
	 */
	private int pageSize = 20;

	/**
	 * The {@link FilenameFilter} implementation that is instantiated in the
	 * {@link #initialize(NamespaceRegistry, NodeTypeManager)} method.
	 */
	// private InclusionExclusionFilenameFilter filenameFilter;

	private NamespaceRegistry registry;

	@Override
	public void initialize(NamespaceRegistry registry,
			NodeTypeManager nodeTypeManager) throws RepositoryException,
			IOException {
		super.initialize(registry, nodeTypeManager);
		this.registry = registry;

		{
			// Init
			ctx = ContextBuilder.newBuilder(provider)
					.credentials(identity, credential)
					.buildView(BlobStoreContext.class);

			if (ctx == null) {
				throw new RepositoryException("ContextBuilder provider = "
						+ provider + " ,identity  " + identity + " failed");
			}

			blobStore = ctx.getBlobStore();

			if (blobStore == null) {
				throw new RepositoryException("getBlobStore provider = "
						+ provider + " ,identity  " + identity + " failed");
			}

		}

	}

	/**
	 * Get the namespace registry.
	 * 
	 * @return the namespace registry; never null
	 */
	NamespaceRegistry registry() {
		return registry;
	}

	/**
	 * Utility method for determining if the supplied identifier is for the
	 * "jcr:content" child node of a file. * Subclasses may override this method
	 * to change the format of the identifiers, but in that case should also
	 * override the {@link #fileFor(String)}, {@link #isRoot(String)}, and
	 * {@link #idFor(File)} methods.
	 * 
	 * @param id
	 *            the identifier; may not be null
	 * @return true if the identifier signals the "jcr:content" child node of a
	 *         file, or false otherwise
	 * @see #isRoot(String)
	 * @see #fileFor(String)
	 * @see #idFor(File)
	 */
	protected boolean isContentNode(String id) {
		return id.endsWith(JCR_CONTENT_SUFFIX);
	}

	// /**
	// * Utility method for obtaining the {@link File} object that corresponds
	// to
	// * the supplied identifier. Subclasses may override this method to change
	// * the format of the identifiers, but in that case should also override
	// the
	// * {@link #isRoot(String)}, {@link #isContentNode(String)}, and
	// * {@link #idFor(File)} methods.
	// *
	// * @param id
	// * the identifier; may not be null
	// * @return the File object for the given identifier
	// * @see #isRoot(String)
	// * @see #isContentNode(String)
	// * @see #idFor(File)
	// */
	// protected File fileFor(String id) {
	// assert id.startsWith(DELIMITER);
	// if (id.endsWith(DELIMITER)) {
	// id = id.substring(0, id.length() - DELIMITER.length());
	// }
	// if (isContentNode(id)) {
	// id = id.substring(0, id.length() - JCR_CONTENT_SUFFIX_LENGTH);
	// }
	// return new File(directory, id);
	// }

	/**
	 * Utility method for determining if the node identifier is the identifier
	 * of the root node in this external source. Subclasses may override this
	 * method to change the format of the identifiers, but in that case should
	 * also override the {@link #fileFor(String)},
	 * {@link #isContentNode(String)}, and {@link #idFor(File)} methods.
	 * 
	 * @param id
	 *            the identifier; may not be null
	 * @return true if the identifier is for the root of this source, or false
	 *         otherwise
	 * @see #isContentNode(String)
	 * @see #fileFor(String)
	 * @see #idFor(File)
	 */
	protected boolean isRoot(String id) {
		return DELIMITER.equals(id);
	}

	// /**
	// * Utility method for determining the node identifier for the supplied
	// file.
	// * Subclasses may override this method to change the format of the
	// * identifiers, but in that case should also override the
	// * {@link #fileFor(String)}, {@link #isContentNode(String)}, and
	// * {@link #isRoot(String)} methods.
	// *
	// * @param file
	// * the file; may not be null
	// * @return the node identifier; never null
	// * @see #isRoot(String)
	// * @see #isContentNode(String)
	// * @see #fileFor(String)
	// */
	// protected String idFor(File file) {
	// String path = file.getAbsolutePath();
	// if (!path.startsWith(directoryAbsolutePath)) {
	// if (directory.getAbsolutePath().equals(path)) {
	// // This is the root
	// return DELIMITER;
	// }
	// String msg =
	// JcrI18n.fileConnectorNodeIdentifierIsNotWithinScopeOfConnector
	// .text(getSourceName(), directoryPath, path);
	// throw new DocumentStoreException(path, msg);
	// }
	// String id = path.substring(directoryAbsolutePathLength);
	// id = id.replaceAll(Pattern.quote(FILE_SEPARATOR), DELIMITER);
	// assert id.startsWith(DELIMITER);
	// return id;
	// }

	protected String blobNameFromUrlId(String urlId) {
		return urlId.substring(providerUrlPrefix.length() + DELIMITER.length()
				+ containerName.length() + DELIMITER.length());
	}

	protected String blobNameFromPath(String path) {
		String id = path;

		if (id.startsWith(DELIMITER)) {
			id = id.substring(DELIMITER.length());

		}
		if (id.endsWith(DELIMITER)) {
			id = id.substring(0, id.length() - DELIMITER.length());
		}
		if (isContentNode(id)) {
			id = id.substring(0, id.length() - JCR_CONTENT_SUFFIX_LENGTH);
		}

		return id;

	}

	// /**
	// * Utility method for creating a {@link BinaryValue} for the given
	// * {@link File} object. Subclasses should rarely override this method.
	// *
	// * @param file
	// * the file; may not be null
	// * @return the BinaryValue; never null
	// */
	// protected ExternalBinaryValue binaryFor(File file) {
	// try {
	// byte[] sha1 = SecureHash.getHash(Algorithm.SHA_1, file);
	// BinaryKey key = new BinaryKey(sha1);
	// return createBinaryValue(key, file);
	// } catch (RuntimeException e) {
	// throw e;
	// } catch (Throwable e) {
	// throw new RuntimeException(e);
	// }
	// }

	protected ExternalBinaryValue binaryFor(String id) {
		try {
			// TODO byte[] sha1 = SecureHash.getHash(Algorithm.SHA_1, file);

			BinaryKey key = new BinaryKey(id);
			id = blobNameFromPath(id);
			return createBinaryValue(key, id);
		} catch (RuntimeException e) {
			throw e;
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Utility method to create a {@link BinaryValue} object for the given id.
	 * Subclasses should rarely override this method, since the
	 * {@link UrlBinaryValue} will be applicable in most situations.
	 * 
	 * @param key
	 *            the binary key; never null
	 * @param file
	 *            the file for which the {@link BinaryValue} is to be created;
	 *            never null
	 * @return the binary value; never null
	 * @throws IOException
	 *             if there is an error creating the value
	 */
	protected ExternalBinaryValue createBinaryValue(BinaryKey key, String id)
			throws IOException {

		return new JCloudsBinaryValue(key, getSourceName(),
				new URL(providerUrlPrefix + DELIMITER + containerName
						+ DELIMITER + id), blobStore, // TODO
				containerName, id, -1, id, getMimeTypeDetector());
	}

	// /**
	// * Construct a {@link URL} object for the given file, to be used within
	// the
	// * {@link Binary} value representing the "jcr:data" property of a
	// * 'nt:resource' node.
	// * <p>
	// * Subclasses can override this method to transform the URL into something
	// * different. For example, if the files are being served by a web server,
	// * the overridden method might transform the file-based URL into the
	// * corresponding HTTP-based URL.
	// * </p>
	// *
	// * @param file
	// * the file for which the URL is to be created; never null
	// * @return the URL for the file; never null
	// * @throws IOException
	// * if there is an error creating the URL
	// */
	// protected URL createUrlForFile(File file) throws IOException {
	// return file.toURI().toURL();
	// }
	//
	// protected File createFileForUrl(URL url) throws URISyntaxException {
	// return new File(url.toURI());
	// }

	// /**
	// * Utility method to determine if the file is excluded by the
	// * inclusion/exclusion filter.
	// *
	// * @param file
	// * the file
	// * @return true if the file is excluded, or false if it is to be included
	// */
	// protected boolean isExcluded(File file) {
	// return !filenameFilter.accept(file.getParentFile(), file.getName());
	// }
	//
	// /**
	// * Utility method to ensure that the file is writable by this connector.
	// *
	// * @param id
	// * the identifier of the node
	// * @param file
	// * the file
	// * @throws DocumentStoreException
	// * if the file is expected to be writable but is not or is
	// * excluded, or if the connector is readonly
	// */
	// protected void checkFileNotExcluded(String id, File file) {
	// if (isExcluded(file)) {
	// String msg = JcrI18n.fileConnectorCannotStoreFileThatIsExcluded
	// .text(getSourceName(), id, file.getAbsolutePath());
	// throw new DocumentStoreException(id, msg);
	// }
	// }

	@Override
	public boolean hasDocument(String id) {

		id = contentNodeId2BlobName(id);

		return JCloudsUtil.blobExists(containerName, blobStore, id) ? true
				: false;
	}

	private String getParentId(String id) {
		int index = id.lastIndexOf(DELIMITER);

		if (id.length() > 1 && index >= 0) {
			id = id.substring(0, index);
			return id.equalsIgnoreCase("") ? DELIMITER : id;

		}
		return null;
	}

	private String getChildName(String id) {
		int index = id.lastIndexOf(DELIMITER);

		if (id.length() > 1 && index >= 0) {
			id = id.substring(index + DELIMITER.length());
		}

		return id;
	}

	private long getLastModified(String blobName) {

		return blobStore.blobExists(containerName, blobName) ? blobStore
				.getBlob(containerName, blobName).getMetadata()
				.getLastModified().getTime() : 0;

	}

	@Override
	public Document getDocumentById(String id) {

		boolean isRoot = isRoot(id);
		DocumentWriter writer = null;
		boolean isResource = isContentNode(id);

		if (isResource) {
			writer = newDocument(id);
			BinaryValue binaryValue = binaryFor(id);
			writer.setPrimaryType(NT_RESOURCE);
			writer.addProperty(JCR_DATA, binaryValue);
			if (addMimeTypeMixin) {
				String mimeType = null;
				String encoding = null; // We don't really know this
				try {
					mimeType = binaryValue.getMimeType();
				} catch (Throwable e) {
					getLogger().error(e, JcrI18n.couldNotGetMimeType,
							getSourceName(), id, e.getMessage());
				}
				writer.addProperty(JCR_ENCODING, encoding);
				writer.addProperty(JCR_MIME_TYPE, mimeType);
			}
			writer.addProperty(JCR_LAST_MODIFIED, factories().getDateFactory()
					.create(getLastModified(blobNameFromPath(id))));
			writer.addProperty(JCR_LAST_MODIFIED_BY, null); // ignored

			// //TODO , in cnd file
			// make these binary not queryable. If we really want to query them,
			// we need to switch to external binaries
			writer.setNotQueryable();
		} else {

			String blobName = blobNameFromPath(id); // "/" folder could not be
													// recognized in clouds
			if (blobName.equals("")
					&& blobStore.blobExists(containerName, blobName)
					|| blobStore.directoryExists(containerName, blobName)) {

				System.out.println("blobStore.directoryExists " + blobName);
				getLogger().trace("blobStore.directoryExists " + blobName);

				blobName = blobName.equals("") ? "/" : blobName;

				writer = newFolderWriter(id, blobName, 0);

			} else if (!blobName.equals("")) {// &&
												// blobStore.blobExists(containerName,
												// blobName)) {
				// TODO mixin support
				writer = newDocument(id);
				writer.setPrimaryType("bagit:cloudsFile");
				writer.addProperty(JCR_CREATED, factories().getDateFactory()
						.create());// TODO
				writer.addProperty(JCR_CREATED_BY, null); // ignored

				writer.addProperty("bagit:absoluteURI", providerUrlPrefix
						+ DELIMITER + containerName + id);

				String childId = isRoot ? JCR_CONTENT_SUFFIX : id
						+ JCR_CONTENT_SUFFIX;
				writer.addChild(childId, JCR_CONTENT);

			} else {
				// wrong id
				System.out.println("blobStore wrong id " + blobName);
				getLogger().trace("blobStore wrong id " + blobName);

				return null;
			}
		}

		if (!isRoot) {
			// Set the reference to the parent ...

			writer.setParents(getParentId(id));
		}

		// Add the extra properties (if there are any), overwriting any
		// properties with the same names
		// (e.g., jcr:primaryType, jcr:mixinTypes, jcr:mimeType, etc.) ...
		writer.addProperties(extraPropertiesStore().getProperties(id));

		// Add the 'mix:mixinType' mixin; if other mixins are stored in the
		// extra properties, this will append ...
		if (addMimeTypeMixin) {
			writer.addMixinType(MIX_MIME_TYPE);
		}

		// Return the document ...
		return writer.document();
	}

	private DocumentWriter newFolderWriter(String path, String id, int offset) {

		long totalChildren = 0;
		int nextOffset = 0;

		boolean root = isRoot(id);

		DocumentWriter writer = newDocument(path);
		writer.setPrimaryType(NT_FOLDER);
		writer.addProperty(JCR_CREATED, null);// ignored
		writer.addProperty(JCR_CREATED_BY, null); // ignored

		PageSet<? extends StorageMetadata> containersRetrieved = root ? blobStore
				.list(containerName) : blobStore.list(containerName,
				ListContainerOptions.Builder.inDirectory(id).maxResults(1000)); // TODO
																				// ,
																				// how
																				// about
																				// too
																				// many
																				// files???

		for (StorageMetadata child : containersRetrieved) {
			if (totalChildren >= offset && totalChildren < offset + pageSize) {
				String childName = child.getName();

				if (childName.equalsIgnoreCase(id)) {
					continue;
				}
				String childId = getChildName(childName);

				// TODO ???
				// if(childId != null && !childId.equals(""))
				// if(!root)
				{
					// writer.addChild(id+childName, childId);
					writer.addChild(DELIMITER + childName, childId);

					nextOffset++;

					System.out.println("child name is " + childName);
				}
			} else {
				break;
			}
			totalChildren++;
		}

		totalChildren = containersRetrieved.size() - 1;

		// if there are still accessible children add the next page
		if (nextOffset < totalChildren) {
			writer.addPage(id, nextOffset, pageSize, totalChildren);
		}

		writer.setNotQueryable();

		return writer;
	}

	@Override
	public String getDocumentId(String path) {

		String id = blobNameFromPath(path);

		return (blobStore.blobExists(containerName, id) || blobStore
				.directoryExists(containerName, id)) ? path : null;
	}

	@Override
	public Collection<String> getDocumentPathsById(String id) {
		// this connector treats the ID as the path
		return Collections.singletonList(id);
	}

	@Override
	public ExternalBinaryValue getBinaryValue(String id) {
		return binaryFor(blobNameFromUrlId(id));
	}

	@Override
	public boolean removeDocument(String id) {
		extraPropertiesStore().removeProperties(id);

		id = contentNodeId2BlobName(id);

		if (!JCloudsUtil.blobExists(containerName, blobStore, id))
			return false;

		JCloudsUtil.remove(containerName, blobStore, id);

		return true;
	}

	private String contentNodeId2BlobName(String id) {

		id = id.startsWith("/") ? id.substring(1) : id;

		id = id.endsWith(JCR_CONTENT_SUFFIX) ? id.substring(0, id.length()
				- JCR_CONTENT_SUFFIX.length()) : id;

		return id;
	}

	@Override
	public void storeDocument(Document document) {
		// Create a new directory or file described by the document ...
		DocumentReader reader = readDocument(document);
		String id = reader.getDocumentId();

		String primaryType = reader.getPrimaryTypeName();
		Map<Name, Property> properties = reader.getProperties();
		ExtraProperties extraProperties = extraPropertiesFor(id, false);
		extraProperties.addAll(properties).except(JCR_PRIMARY_TYPE,
				JCR_CREATED, JCR_LAST_MODIFIED, JCR_DATA);
		try {
			if (NT_FILE.equals(primaryType)) {
				// TODO ...
			} else if (NT_FOLDER.equals(primaryType)) {
				// TODO ...
			} else if (isContentNode(id)) {
				Property content = properties.get(JcrLexicon.DATA);
				BinaryValue binary = factories().getBinaryFactory().create(
						content.getFirstValue());

				JCloudsUtil.write(containerName, blobStore, binary,
						contentNodeId2BlobName(id));

				if (!NT_RESOURCE.equals(primaryType)) {
					// This is the "jcr:content" child, but the primary type is
					// non-standard so record it as an extra property
					extraProperties
							.add(properties.get(JcrLexicon.PRIMARY_TYPE));
				}
			}
			extraProperties.save();
		} catch (Exception e) {
			throw new DocumentStoreException(id, e);
		}
	}

	@Override
	public String newDocumentId(String parentId, Name newDocumentName,
			Name newDocumentPrimaryType) {
		StringBuilder id = new StringBuilder(parentId);
		if (!parentId.endsWith(DELIMITER)) {
			id.append(DELIMITER);
		}

		// We're only using the name to check, which can be a bit dangerous if
		// users don't follow the JCR conventions.
		// However, it matches what "isContentNode(...)" does.
		String childNameStr = getContext().getValueFactories()
				.getStringFactory().create(newDocumentName);
		if (JCR_CONTENT.equals(childNameStr)) {
			// This is for the "jcr:content" node underneath a file node. Since
			// this doesn't actually result in a file or folder
			// on the file system (it's merged into the file for the parent
			// 'nt:file' node), we'll keep the "jcr" namespace
			// prefix in the ID so that 'isContentNode(...)' works properly ...
			id.append(childNameStr);
		} else {
			// File systems don't universally deal well with ':' in the names,
			// and when they do it can be a bit awkward. Since we
			// don't often expect the node NAMES to contain namespaces (at leat
			// with this connector), we'll just
			// use the local part for the ID ...
			id.append(newDocumentName.getLocalName());
			if (!StringUtil.isBlank(newDocumentName.getNamespaceUri())) {
				// the FS connector does not support namespaces in names
				String ns = newDocumentName.getNamespaceUri();
				getLogger().warn(JcrI18n.fileConnectorNamespaceIgnored,
						getSourceName(), ns, id, childNameStr, parentId);
			}
		}
		return id.toString();
	}

	@Override
	public void updateDocument(DocumentChanges documentChanges) {
		String id = documentChanges.getDocumentId();

		Document document = documentChanges.getDocument();
		DocumentReader reader = readDocument(document);
		String primaryType = reader.getPrimaryTypeName();
		Map<Name, Property> properties = reader.getProperties();
		ExtraProperties extraProperties = extraPropertiesFor(id, true);
		extraProperties.addAll(properties).except(JCR_PRIMARY_TYPE,
				JCR_CREATED, JCR_LAST_MODIFIED, JCR_DATA);
		try {
			if (NT_FILE.equals(primaryType)) {
				// TODO file.createNewFile();
			} else if (NT_FOLDER.equals(primaryType)) {
				// TODO file.mkdir();
			} else if (isContentNode(id)) {
				Property content = reader.getProperty(JCR_DATA);
				BinaryValue binary = factories().getBinaryFactory().create(
						content.getFirstValue());

				JCloudsUtil.write(containerName, blobStore, binary,
						contentNodeId2BlobName(id));

				if (!NT_RESOURCE.equals(primaryType)) {
					// This is the "jcr:content" child, but the primary type is
					// non-standard so record it as an extra property
					extraProperties
							.add(properties.get(JcrLexicon.PRIMARY_TYPE));
				}
			}
			extraProperties.save();
		} catch (Exception e) {
			throw new DocumentStoreException(id, e);
		}
	}

	// @Override
	public Document getChildren(PageKey pageKey) {
		// TODO ...
		return null;

	}

	/**
	 * Shutdown the connector by releasing all resources. This is called
	 * automatically by ModeShape when this Connector instance is no longer
	 * needed, and should never be called by the connector.
	 */
	public void shutdown() {
		getLogger().debug("shutdown is invoked. ");

		if (ctx != null) {
			ctx.close();
		}

	}

}
