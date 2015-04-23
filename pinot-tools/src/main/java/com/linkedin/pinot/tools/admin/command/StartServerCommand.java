/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.tools.admin.command;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.kohsuke.args4j.Option;

import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.server.starter.helix.HelixServerStarter;

/**
 * Class to implement StartServer command.
 *
 * @author Mayank Shrivastava <mshrivastava@linkedin.com>
 */
public class StartServerCommand extends AbstractBaseCommand implements Command {
  @Option(name="-clusterName", required=true, metaVar="<string>", usage="Name of the cluster.")
  private String _clusterName = null;

  @Option(name="-zkAddress", required=true, metaVar="<http>", usage="Http address of Zookeeper.")
  private String _zkAddress = null;

  @Option(name="-dataDir", required=true, metaVar="<string>", usage="Path to directory containing data.")
  private String _dataDir = null;

  @Option(name="-segmentDir", required=true, metaVar="<string>", usage="Path to directory containing segments.")
  private String _segmentDir = null;

  //DEFAULT_SERVER_NETTY_PORT
  @Option(name="-port", required=false, metaVar="<int>", usage="Port number to start the server at.")
  private int _port = CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT;

  @Option(name="-help", required=false, help=true, usage="Print this message.")
  private boolean _help = false;

  public boolean getHelp() {
    return _help;
  }

  public void init(String clusterName, String zkAddress, String dataDir, String segmentDir) {
    _clusterName = clusterName;
    _zkAddress = zkAddress;

    _dataDir = dataDir;
    _segmentDir = segmentDir;
  }

  @Override
  public String toString() {
    return ("StartServerCommand -clusterName " + _clusterName + " -zkAddress " + _zkAddress +
        " -dataDir " + _dataDir + " -segmentDir " + _segmentDir);
  }

  @Override
  public String getName() {
    return "StartServer";
  }

  @Override
  public void cleanup() {

  }

  @Override
  public boolean execute() throws Exception {
    final Configuration configuration = new PropertiesConfiguration();

    configuration.addProperty(CommonConstants.Helix.KEY_OF_SERVER_NETTY_PORT, _port);
    configuration.addProperty("pinot.server.instance.dataDir", _dataDir + _port + "/index");
    configuration.addProperty("pinot.server.instance.segmentTarDir", _segmentDir + _port + "/segmentTar");

    final HelixServerStarter pinotHelixStarter =
        new HelixServerStarter(_clusterName, _zkAddress, configuration);

    savePID("/tmp/.pinotAdminServer.pid");
    return true;
  }
}
