/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.launcher.perJob;

import com.dtstack.flinkx.launcher.YarnConfLoader;
import com.dtstack.flinkx.options.Options;
import com.dtstack.flinkx.util.MapUtil;
import com.dtstack.flinkx.util.SysUtil;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Properties;

import static com.dtstack.flinkx.launcher.Launcher.*;

/**
 * Date: 2019/09/11 Company: www.dtstack.com
 *
 * @author tudou
 */
public class PerJobSubmitter {
    private static final Logger LOG = LoggerFactory.getLogger(PerJobSubmitter.class);

    /**
     * submit per-job task
     *
     * @param launcherOptions LauncherOptions
     * @param remoteArgs remoteArgs
     * @throws Exception
     */
    public static String submit(Options launcherOptions, String[] remoteArgs) throws Exception {
        LOG.info("start to submit per-job task, launcherOptions = {}", launcherOptions.toString());
        Properties conProp =
                MapUtil.jsonStrToObject(launcherOptions.getConfProp(), Properties.class);
        ClusterSpecification clusterSpecification =
                FlinkPerJobUtil.createClusterSpecification(conProp);
        clusterSpecification.setCreateProgramDelay(true);

        String pluginRoot = launcherOptions.getPluginRoot();
        File jarFile = new File(pluginRoot + File.separator + getCoreJarFileName(pluginRoot));
        clusterSpecification.setConfiguration(launcherOptions.loadFlinkConfiguration());
        List<URL> usrClasspath = analyzeUserClasspath(launcherOptions.getJob(), pluginRoot);
        usrClasspath.addAll(findFlinkDistLibJar(launcherOptions.getFlinkLibJar()));
        clusterSpecification.setClasspaths(usrClasspath);
        clusterSpecification.setEntryPointClass(MAIN_CLASS);
        clusterSpecification.setJarFile(jarFile);

        if (StringUtils.isNotEmpty(launcherOptions.getS())) {
            clusterSpecification.setSpSetting(
                    SavepointRestoreSettings.forPath(launcherOptions.getS()));
        }
        clusterSpecification.setProgramArgs(remoteArgs);
        clusterSpecification.setCreateProgramDelay(true);
        clusterSpecification.setYarnConfiguration(
                YarnConfLoader.getYarnConf(launcherOptions.getYarnconf()));
        PerJobClusterClientBuilder perJobClusterClientBuilder = new PerJobClusterClientBuilder();
        perJobClusterClientBuilder.init(launcherOptions, conProp);

        YarnClusterDescriptor descriptor =
                perJobClusterClientBuilder.createPerJobClusterDescriptor(launcherOptions);

        ClusterClientProvider<ApplicationId> provider =
                descriptor.deployJobCluster(clusterSpecification, new JobGraph("df-flinkx"), true);
        String applicationId = provider.getClusterClient().getClusterId().toString();
        String flinkJobId = clusterSpecification.getJobGraph().getJobID().toString();
        LOG.info("deploy per_job with appId: {}, jobId: {}", applicationId, flinkJobId);
        return applicationId;
    }

    private static List<URL> findFlinkDistLibJar(String flinkLibJarDir) {
        if (StringUtils.isNotBlank(flinkLibJarDir)) {
            if (!new File(flinkLibJarDir).exists()) {
                throw new IllegalArgumentException("The Flink jar path is not exist");
            }
        } else {
            throw new IllegalArgumentException("The Flink jar path is null");
        }
        try {
            return SysUtil.findJarsInDir(new File(flinkLibJarDir));
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }
}
