fcrepo-modeshape-jcloudsconnector
=================================
A modeshape connector to connect to the most clouds providers via JClouds lib.

For more infomation, please referrence:
1) https://github.com/futures
2) https://github.com/ModeShape/modeshape
3) https://github.com/jclouds/jclouds


You also need Clouds account to run this connector;

For testing, you need adding below function in JcrTools.java in package org.modeshape.jcr.api;

    public Node uploadFile( Session session,
                            String path,
                            InputStream stream,
                            String defaultNodeType,
                            String finalNodeType) throws RepositoryException, IOException {
        isNotNull(session, "session");
        isNotNull(path, "path");
        isNotNull(stream, "stream");
        Node fileNode = null;
        boolean error = false;
        try {
            // Create an 'nt:file' node at the supplied path, creating any missing intermediate nodes of type 'nt:folder' ...
            fileNode = findOrCreateNode(session.getRootNode(), path, defaultNodeType, finalNodeType);

            // Upload the file to that node ...
            Node contentNode = findOrCreateChild(fileNode, "jcr:content", "nt:resource");
            Binary binary = session.getValueFactory().createBinary(stream);
            contentNode.setProperty("jcr:data", binary);
        } catch (RepositoryException e) {
            error = true;
            throw e;
        } catch (RuntimeException e) {
            error = true;
            throw e;
        } finally {
            try {
                stream.close();
            } catch (RuntimeException e) {
                if (!error) throw e; // don't override any exception thrown in the block above
            }
        }
        return fileNode;
    }
