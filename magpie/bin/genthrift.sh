#!/bin/sh
#  Copyright (c) 2013 Yahoo! Inc. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.
#
rm -rf gen-javabean 
generated_path=com/jd/bdp/magpie/generated
mkdir -p java/$generated_path
rm -rf java/$generated_path
thrift --gen java:beans,nocamel nimbus.thrift
mv gen-javabean/$generated_path/ java/$generated_path
rm -rf gen-javabean
