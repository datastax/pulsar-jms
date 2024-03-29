#
# Copyright (c) 2009, 2018 Oracle and/or its affiliates. All rights reserved.
#
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License v. 2.0, which is available at
# http://www.eclipse.org/legal/epl-2.0.
#
# This Source Code may also be made available under the following Secondary
# Licenses when the conditions for such availability set forth in the
# Eclipse Public License v. 2.0 are satisfied: GNU General Public License,
# version 2 with the GNU Classpath Exception, which is available at
# https://www.gnu.org/software/classpath/license.html.
#
# SPDX-License-Identifier: EPL-2.0 OR GPL-2.0 WITH Classpath-exception-2.0
#

#
# $Id: ts.jte 69480 2013-01-11 18:52:37Z af70133 $
#

####################################################################################
#                                                                   	           #
#   JavaTest Environment file for JMS Compatibility Test Suite                     #
#                                                                                  #
#   Environment specific properties in this file will likely	                   #
#   have to be modified prior to running the JMS TCK. Instructions                 #
#   for modifying these properties are contained in this file.	                   #
#                                                                                  #
####################################################################################

####################################################################################
# Properties to set prior to running the JMS TCK tests		      
# @jms.home		-- Install directory of the JMS Reference Implementation.
# @jms.classes		-- Classes of the JMS Reference Implementation needed to
#			   run and/or build the JMS TCK. If testing with the JMS
#			   Reference Implementation then set jms.classes to the
#			   following:
#
#			   jms.classes=${ri.jars}
#
# See the bottom of this ts.jte file for properties specific to the JMS Reference 
# Implementation. The variable "ri.jars" at bottom of ts.jte file hardcodes the 
# classes/jars for the JMS Reference Implementation. 
#
# The JMS Reference Implementation and the Open Message Queue Product are the 
# Standalone JMS RI implementations. The JMS TCK has been run against both.
####################################################################################
jms.home=/Users/enrico.olivelli/dev/pulsar-jms/tck-executor
jms.classes=/Users/enrico.olivelli/.m2/repository/jakarta/jms/jakarta.jms-api/2.0.3/jakarta.jms-api-2.0.3.jar:${jms.home}/target/tck-executor-1.0.0.jar

####################################################################################
# The jars/classes for the TS harness/javatest and the test classes
# @ts.harness.classpath --  Classes required by javatest
# @ts.classpath         --  Classes used to build the CTS tests
# @ts.run.classpath     --  Classpath required by the vendor
#                           appclient container.
####################################################################################
ts.harness.classpath=${ts.home}/lib/javatest.jar${pathsep}${ts.home}/lib/tsharness.jar${pathsep}${ts.home}/lib/jmstck.jar${pathsep}${ant.home}/lib/ant.jar${pathsep}${ts.home}/lib/jaxb-api.jar${pathsep}${ts.home}/lib/jaxb-xjc.jar${pathsep}${ts.home}/lib/jaxb-libs.jar${pathsep}${ts.home}/lib/jaxb-impl.jar

ts.classpath=${jms.classes}${pathsep}${ts.home}/lib/tsharness.jar${pathsep}${ts.home}/lib/jmstck.jar

ts.run.classpath=${ts.home}/lib/tsharness.jar${pathsep}${ts.home}/lib/sigtest.jar${pathsep}${ts.home}/lib/jmstck.jar${pathsep}${ts.home}/classes${pathsep}${ts.home}/bin${pathsep}${jms.classes}

###############################################################################
# @tools.jar  should be set to the location of the tools.jar from the 
# installed jdk
###############################################################################
tools.jar=${jdk.home}/lib/tools.jar

#############################################################################
# @sigTestClasspath: This property must be set when running signature 
#                    tests.  This property should be set to a list of
#                    jar files and/or directories which contain your 
#                    Java EE and Java SE classes. Paths must be 
#                    separated by the appropriate path separator 
#                    (';' windows, ':' Unixes).
#############################################################################
sigTestClasspath=${jms.classes}${pathsep}${JAVA_HOME}/lib/rt.jar

############################################################################
# These properties are used by the harness.  "harness.log.port"
# specifies the port that server components use to send logging
# output back to JavaTest.  If the default port # is not available
# on the machine running JavaTest, then you can set it here.
#
# @harness.temp.directory directory location used by the
#                         harness to store temporary files
#
# @harness.log.port  the port the harness listens on for log mesages
#                    from remote clients
#
# @harness.log.traceflag  used to turn on/off verbose debugging output
#                         for the tests.
#
# @harness.log.delayseconds  Number of seconds to delay to allow
#                            reporting from remote clients to finish.
#
# @harness.executeMode  used to run the harness in the following modes
#                       of execution:  2 - run only
#
# @harness.socket.retry.count - denotes the number of time we should
#           attempt to create a server socket when intilizing a test
#           client. The socket is used for logging purposes.
############################################################################
harness.temp.directory=${ts.home}/tmp
harness.log.port=2001
harness.log.traceflag=false
harness.log.delayseconds=1 
harness.executeMode=2
harness.socket.retry.count=10
work.dir=/tmp/JTwork
report.dir=/tmp/JTreport
if.existing.work.report.dirs=auto

###########################################################################
# @tz - your local timezone. For valid values, consult your
#       Operating System documentation.
###########################################################################
tz=Europe/Rome

#############################################################################
# @javatest.timeout.factor This property specifies the scale factor used by
# Javatest to adjust the time JavaTest will wait for a given test to
# complete before returning failure.  For instance if the default test timeout
# is 5 minutes, this value will be multiplied by 5 minutes to determine
# the total timeout delay.  Note: this value only works with Javatest's
# batch mode (runclient).  When using the Javatest GUI users must change
# this timeout factor in the GUI. Configure -> Edit Configuration -> View
# -> choose Standard Values -> select tab Execution ->  set Time Factor.
#############################################################################
javatest.timeout.factor=1.0

#######################################################################
#build level
#######################################################################
#1: compile only
#2: compile and build component archives (e.g., jar's, war's)
build.level=1

###############################################################
# @ts.display -- location to display CTS output on Unix
###############################################################
ts.display=:0.0

#######################################################################
# default deliverable class
#######################################################################
deliverable.class=com.sun.ts.lib.deliverable.jms.JMSDeliverable

#####################################################################
## This command allows the test clients to be run in
## the same JVM as JavaTest in this case.  This mode can be
## enabled by passing "-Dsame.jvm=true" to the runclient or gui
## targets.  NOTE: This option is only to be used for sanity
## checking and not when running the TCK for compatibility.
#####################################################################
command.testExecuteSameJVM=com.sun.ts.lib.harness.ExecuteTSTestSameJVMCmd \
        $testExecuteClass $testExecuteArgs

########################################################################
## Common environment for both ts_unix and ts_win32
########################################################################
command.testExecute=com.sun.ts.lib.harness.ExecTSTestCmd \
        CLASSPATH=${ts.run.classpath} \
        DISPLAY="${ts.display}" \
		HOME="${user.home}"  \
		windir=${windir} \
		SYSTEMROOT=${SYSTEMROOT} \
        ${JAVA_HOME}/bin/java \
		-Djava.security.policy="${bin.dir}/harness.policy" \
		-Djava.security.manager \
		-Dlog4j2.debug=true \
		-Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
		-Djava.naming.factory.initial=${java.naming.factory.initial} \
		-Ddeliverable.class=${deliverable.class} \
                -Djava.endorsed.dirs=${endorsed.dirs} \
        $testExecuteClass $testExecuteArgs

#########################################################################
## Environment for ts_unix
## The test execution commands inherit from common environment
## defined above: testExecute.
## If you need to override it, uncomment it in the following section.
#########################################################################
env.ts_unix.menu=true

########################################################################
## Environment for ts_win32
## The test execution commands inherit from common environment
## defined above: testExecute.
## If you need to override it, uncomment it in the following section.
########################################################################
env.ts_win32.menu=true

########################################################################
# This property is used by porting class to set JNDI name
########################################################################
porting.ts.jmsObjects.class.1=com.sun.ts.lib.implementation.sun.jms.SunRIJMSObjects

###############################################################
#These properties are needed for the JMS tests.
###############################################################
jms_timeout=3000
user=j2ee
password=j2ee

######################################################################
## Deliverables must set this property to the name of the deliverable
## specific library jar file (iff create.manifest.extension.entries
## is set to true)
######################################################################
tslib.name=jmstck

#########################################################################
# platform.mode is used by to specify runtime environment for testing
# which can be either javaEE or standalone. For JMS TCK this setting
# is standalone.
#
# platform.mode=standalone     Testing JMS TCK as Standalone environment
#########################################################################
platform.mode=standalone

#######################################################################################
# JMS RI Implementation Property Settings for Vendor Implementation under test
#
# The TCK uses the following JMS RI Implementations:
#   - ri	  (For the JMS Reference Implementation or Open Message Queue Product)
#
# Here are the common properties that need to be defined for the common implementation
# functionality:
#
# @impl.vi    	This property must be set to the Vendor implementation under test.
#
#		Default value is (ri) for the Standalone JMS Reference Implementation 
#		or the Open Message Queue Product (impl.vi=ri).
#######################################################################################

impl.vi=ri

# ---------------------------------------------------------------------------------
# ================================================
# Standalone JMS Reference Implementation and Open
# Message Queue Product Configuration Properties
# ================================================
#
# Property settings for ant config.vi/clean.vi tasks to configure, unconfigure, 
# setup, cleanup, start, and stop the Standalone JMS Reference Implementation 
# and the Open Message Queue Product for a JMS TCK run.
#
# These properties require NO CHANGES other than adding the drive letter to the
# (admin.pass.file) and (jndi.fs.dir) properties if you are running on a WINDOWS
# based system. The settings below are for UNIX based systems. For WINDOWS the
# settings MUST BE:
#
# admin.pass.file=C:/tmp/ripassword
# jndi.fs.dir=C:/tmp/ri_admin_objects
#
# Finally you need to edit the file "${ts.home}/bin/xml/impl/ri/jndi.properties"
# file and include the drive letter in the (java.naming.provider.url) property
# if you are running on a WINDOWS based system. For WINDOWS the setting MUST BE:
#
# java.naming.provider.url=file:///C:/tmp/ri_admin_objects
# 
# The JMS TCK uses the JNDI Filesystem Context Provider as the JMS Object store
# for the JMS administered objects for the Standalone JMS Reference Implementation
# and the Open Message Queue Product.
# 
# For the Standalone JMS Reference Implementation and the Open Message Queue 
# Product all you need to set is the "jms.home" property at the top of this 
# file to the location where the Standalone JMS Reference Implementation or 
# Open Message Queue Product is installed. Then you to set the "jms.classes" 
# property to the location of the classes/jars of the Standalone JMS Reference 
# Implementation or the Open Message Queue Product. The classes/jars are 
# hardcoded in the property "ri.jars" below. So set "jms.classes="${ri.jars}"
# and you are now set to run the JMS TCK against the Standalone JMS Reference 
# Implementation or the Open Message Queue Product.
#####################################################################
# ---------------------------------------------------------------------------------
#
ri.home=${jms.home}
ri.jars=${jms.home}/target/tck-executor-1.0.0-SNAPSHOT.jar
admin.user=admin
admin.pass=admin
admin.pass.file=/tmp/ripassword
#jndi.fs.dir=/tmp/ri_admin_objects
java.naming.factory.initial="com.datastax.oss.pulsar.jms.tests.JNDIInitialContextFactory"
#jndi.provider.url="java.naming.provider.url=file:///${jndi.fs.dir}"
