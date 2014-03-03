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

package org.fcrepo.federation.jcloudsconnector;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
//import java.io.File;
//import java.io.FileOutputStream;
//import java.io.IOException;
//import java.io.InputStream;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
import javax.jcr.Node;
//import javax.jcr.NodeIterator;
//import javax.jcr.PathNotFoundException;
import javax.jcr.Property;
//import javax.jcr.RepositoryException;
//import javax.jcr.Workspace;
import org.junit.Before;
import org.junit.Test;
//import org.modeshape.common.FixFor;
//import org.modeshape.common.annotation.Immutable;
//import org.modeshape.common.util.FileUtil;
//import org.modeshape.common.util.IoUtil;
import org.modeshape.jcr.SingleUseAbstractTest;
import org.modeshape.jcr.api.Binary;
import org.modeshape.jcr.api.JcrTools;
import org.modeshape.jcr.api.Session;

public class JCloudsConnectorTest extends SingleUseAbstractTest {

	protected static void printDocumentView(Session session, String path)
			throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		session.exportSystemView(path, baos, false, false);

		System.out.println(baos);

	}

	protected static final String TEXT_CONTENT = "Some text content in jcloudsfile";

	private JcrTools tools;

	public JCloudsConnectorTest() {

	}

	@Before
	public void before() throws Exception {
		tools = new JcrTools();
		startRepositoryWithConfiguration(getClass().getClassLoader()
				.getResourceAsStream("repo-config-federation-projections.json"));
		registerNodeTypes("bagitCloudFile.cnd");

	}

	@Test
	public void shouldAccessBinaryContent() throws Exception {

		printDocumentView(session, "/bags");
		Node file = session.getNode("/bags/asf.png");
		Node cnt = file.getNode("jcr:content");

		Property value = cnt.getProperty("jcr:data");

		Binary bv = (Binary) value.getValue().getBinary();
		InputStream is = bv.getStream();

		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		int b = 0;
		while (b != -1) {
			b = is.read();
			if (b != -1) {
				bout.write(b);
			}
		}

	}

	@Test
	public void shouldAllowCreatingNode() throws Exception {
		String actualContent = "This is the content of the file.";
		tools.uploadFile(session, "/bags/asfnew.txt", new ByteArrayInputStream(
				actualContent.getBytes()), "nt:folder", "bagit:cloudsFile");
		session.save();
	}

	@Test
	public void shouldAllowDeletingNode() throws Exception {

		session.refresh(false);
		Node node = session.getNode("/bags/asfnew.txt");
		node.remove();
		session.save();

	}

}
