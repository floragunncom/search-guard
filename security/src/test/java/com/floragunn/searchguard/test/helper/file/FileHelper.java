/*
 * Copyright 2015-2017 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.floragunn.searchguard.test.helper.file;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.*;

import java.io.*;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;

public class FileHelper {

	protected final static Logger log = LogManager.getLogger(FileHelper.class);

	public static KeyStore getKeystoreFromClassPath(final String fileNameFromClasspath, String password) throws Exception {
	    Path path = getAbsoluteFilePathFromClassPath(fileNameFromClasspath);
	    if(path==null) {
	        return null;
	    }

	    KeyStore ks = KeyStore.getInstance("JKS");
	    try (FileInputStream fin = new FileInputStream(path.toFile())) {
	        ks.load(fin, password==null||password.isEmpty()?null:password.toCharArray());
	    }
	    return ks;
	}

    public static Path getAbsoluteFilePathFromClassPath(String fileNameFromClasspath) throws FileNotFoundException {
        return getAbsoluteFilePathFromClassPath(null, fileNameFromClasspath);
    }

	public static File createDirectoryInResources(String directory) {
		try {
			URL tempCertUrl = FileHelper.class.getClassLoader().getResource("");
			File resourceDirectory = new File(URLDecoder.decode(tempCertUrl.getFile(), "UTF-8"));
			resourceDirectory = new File(resourceDirectory, directory);

			if (!resourceDirectory.exists()) {
				boolean mkdirCreated = resourceDirectory.mkdir();
				if(!mkdirCreated) {
					throw new RuntimeException(String.format("Directory creation failed for: %s", resourceDirectory));
				}
			}
			return resourceDirectory;
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("Getting resources directory failed for: " + directory, e);
		}
	}

	public static Path getAbsoluteFilePathFromClassPath(String resourceDirectory, String fileNameFromClasspath) throws FileNotFoundException {
        if (resourceDirectory != null) {
            if (resourceDirectory.endsWith("/")) {
                fileNameFromClasspath = resourceDirectory + fileNameFromClasspath;
            } else {
                fileNameFromClasspath = resourceDirectory + "/" + fileNameFromClasspath;
            }
        }

		URL fileUrl = FileHelper.class.getClassLoader().getResource(fileNameFromClasspath);

		if (fileUrl == null) {
		    throw new FileNotFoundException("Could not locate " + fileNameFromClasspath);
		}

		if (fileUrl.getProtocol().equals("file")) {
            try {
                File file = new File(URLDecoder.decode(fileUrl.getFile(), "UTF-8"));

                if (!file.exists()) {
                    throw new FileNotFoundException("Could not locate " + fileNameFromClasspath + " at " + file);
                }

                return Paths.get(file.getAbsolutePath());
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        } else if (fileUrl.getProtocol().equals("jar")) {
            try {
                File tempFile = File.createTempFile(FilenameUtils.getBaseName(fileNameFromClasspath),
                        FilenameUtils.getExtension(fileNameFromClasspath));

                FileUtils.copyInputStreamToFile(fileUrl.openStream(), tempFile);

                return tempFile.toPath();
            } catch (IOException e) {
                throw new RuntimeException("Error while making " + fileNameFromClasspath + " available as temp file", e);
            }
        } else {
            throw new RuntimeException("Unsupported scheme " + fileUrl);
        }
	}

	public static final String loadFile(final String file) throws IOException {
		final StringWriter sw = new StringWriter();

		InputStream is = FileHelper.class.getResourceAsStream("/" + file);

		if (is == null) {
		    throw new FileNotFoundException("Could not find resource in class path: " + file);
		}

		IOUtils.copy(is, sw, StandardCharsets.UTF_8);
		return sw.toString();
	}

	public static void writeFile(String destFile, String content) throws IOException {
	    FileWriter fw = new FileWriter(destFile, false);
	    fw.write(content);
	    fw.close();
	}

    public static BytesReference readYamlContent(final String file) {

		try (XContentParser parser = XContentFactory.xContent(XContentType.YAML).createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, new StringReader(loadFile(file)))) {
			parser.nextToken();
			final XContentBuilder builder = XContentFactory.jsonBuilder();
			builder.copyCurrentStructure(parser);
			return BytesReference.bytes(builder);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		//ignore
	}

    public static BytesReference readYamlContentFromString(final String yaml) {

        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(XContentType.YAML).createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, new StringReader(yaml));
            parser.nextToken();
            final XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.copyCurrentStructure(parser);
            return BytesReference.bytes(builder);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            if (parser != null) {
                try {
                    parser.close();
                } catch (IOException e) {
                    //ignore
                }
            }
        }
    }

	public static void copyFileContents(String srcFile, String destFile) {
		try {
			final FileReader fr = new FileReader(srcFile);
			final BufferedReader br = new BufferedReader(fr);
			final FileWriter fw = new FileWriter(destFile, false);
			String s;

			while ((s = br.readLine()) != null) { // read a line
				fw.write(s); // write to output file
				fw.write(System.getProperty("line.separator"));
				fw.flush();
			}

			br.close();
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
