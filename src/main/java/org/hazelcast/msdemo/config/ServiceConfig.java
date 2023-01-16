/*
 * Copyright 2018-2022 Hazelcast, Inc
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.hazelcast.msdemo.config;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class ServiceConfig {

    static Map<String,ServiceProperties> configurations = new HashMap<>();

    public static class ServiceProperties {
        public String service_name;
        public String grpc_hostname;
        public String grpc_port; // actually numeric
        public String hz_mode;
        public String hz_client_config;
        public String database_host;

        // Format used by gRPC ManagedChannelBuilder.forTarget()
        public String getTarget() {
            return grpc_hostname + ":" + grpc_port;
        }

        public String getGrpcHostname() { return grpc_hostname; }
        public int getGrpcPort() {
            return Integer.parseInt(grpc_port);
        }
        // Could be hostname or IP
        public String getDatabaseHost() { return database_host; }
        //public String getClientConfigFilename() { return  hz_client_config; }
        public byte[] getClientConfig() {
            if (isEmbedded() || hz_client_config == null) {
                return new byte[0];
            }
            System.out.println("ServiceConfig.getClientConfig for " + service_name + ": " + hz_client_config);
            try {
                URL configURL = ServiceConfig.class.getClassLoader().getResource(hz_client_config);
                byte[] bytes = configURL.openStream().readAllBytes();
                return bytes;
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }
        private URL getClientConfigURL() {
            // OK for this to be null for services that are always in embedded mode
            if (hz_client_config == null) {
                return null;
            }
            ClassLoader classloader = Thread.currentThread().getContextClassLoader();
            URL url = classloader.getResource(hz_client_config);
            System.out.println("Specified client config as url: " + url.toString());
            return url;
        }

        public boolean isEmbedded() {
            validateMode();
            return hz_mode.equalsIgnoreCase("embedded");
        }
        public boolean isClientServer() {
            validateMode();
            return hz_mode.equalsIgnoreCase("client-server");
        }
        private boolean validateMode() {
            if (hz_mode.equalsIgnoreCase("embedded") )
                return true;
            if (hz_mode.equalsIgnoreCase("client-server"))
                return true;
            // Should we allow a default here - and if so, which?  Probably embedded
            if (hz_mode == null)
                throw new IllegalArgumentException(("hz-mode not set"));
            else
                throw new IllegalArgumentException(("Illegal hz_mode setting " + hz_mode +
                        " (valid options are embedded or client-server)"));

        }
    }

//    static {
//        read();
//    }

    public static ServiceProperties get(String serviceName) {
        return get(serviceName, null);
    }

    // In some cases (dataload in particular) using the default classloader pulls in the
    // wrong service.yaml, so it is safest to pass in a CL associated with the module that
    // is home to the correct service.yaml file.
    public static ServiceProperties get(String serviceName, ClassLoader classLoaderForResources) {
        return get("service.yaml", serviceName, classLoaderForResources);
    }

    public static ServiceProperties get(String filename, String serviceName, ClassLoader classLoaderForResources) {
        read(classLoaderForResources, filename);
        return configurations.get(serviceName);
    }

    // Open question as to whether we should clear configurations map when this runs ...
    //
    static private void read(ClassLoader classLoaderForServiceYaml, String filename) {
        if (classLoaderForServiceYaml == null)
            classLoaderForServiceYaml = ServiceConfig.class.getClassLoader();
        YAMLFactory yfactory = new YAMLFactory();
        ObjectMapper mapper = new ObjectMapper(yfactory);
        MappingIterator<ServiceProperties> configInfo;
        try {

//            // DEBUG
//            // May have multiple service.yamls along classpath - are we getting wrong one?
//            InputStream is = classLoaderForServiceYaml.getResourceAsStream(filename);
//            InputStreamReader isr = new InputStreamReader(is);
//            BufferedReader br = new BufferedReader(isr);
//            while (br.ready()) {
//                System.out.println(" :" + br.readLine());
//            }
//            // !DEBUG

            URL yamlFile = classLoaderForServiceYaml.getResource(filename);
            YAMLParser parser = yfactory.createParser(yamlFile);
            System.out.println("ServiceConfig reading config info from " + yamlFile.toExternalForm());
            configInfo = mapper.readValues(parser, ServiceProperties.class);
            int configsLoaded = 0;
            while (configInfo.hasNext()) {
                ServiceProperties sp = configInfo.next();
                configurations.put(sp.service_name, sp);
                configsLoaded++;
                System.out.println(" -- " + sp.service_name);
            }
            System.out.println("ServiceConfig loaded " + configsLoaded + " service definitions, now has " + configurations.size());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // For testing use only.  Needs service-yaml.test renamed to drop '.test' in order to work.
    // (Renamed because otherwise it overwrote service-specific files when jar with dependencies built)
    public static void main(String[] args) {
        //ServiceConfig serviceConfig = new ServiceConfig();
        ServiceProperties props = get("test-service-1");
        System.out.println("Test service 1 can be found at " + props.grpc_hostname + ":" + props.grpc_port);
        if (! (props.isClientServer() || props.isEmbedded()) )
            throw new IllegalArgumentException(("HZ mode not set or set to illegal value"));
        props = get("test-service-2");
        System.out.println("Test service 2 can be found at " + props.grpc_hostname + ":" + props.grpc_port);
        if (! (props.isClientServer() || props.isEmbedded()) )
            throw new IllegalArgumentException(("HZ mode not set or set to illegal value"));
    }
}
