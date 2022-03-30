/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.zeppelin.kotlin.KotlinInterpreter;

import java.io.File;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is bridge class which bridge the communication between java side and scala side.
 * Java side reply on this abstract class which is implemented by different scala versions.
 */
public abstract class AbstractSparkScalaInterpreter {

  private static AtomicInteger SESSION_NUM = new AtomicInteger(0);

  protected SparkConf conf;
  protected SparkContext sc;
  protected SparkSession sparkSession;
  protected SQLContext sqlContext;
  protected String sparkUrl;
  protected ZeppelinContext z;

  public SparkContext getSparkContext() {
    return this.sc;
  }

  public SQLContext getSqlContext() {
    return this.sqlContext;
  }

  public Object getSparkSession() {
    return this.sparkSession;
  }

  public String getSparkUrl() {
    return this.sparkUrl;
  }

  public ZeppelinContext getZeppelinContext() {
    return this.z;
  }

  public AbstractSparkScalaInterpreter() {
  }

  protected void open() {
    /* Required for scoped mode.
     * In scoped mode multiple scala compiler (repl) generates class in the same directory.
     * Class names is not randomly generated and look like '$line12.$read$$iw$$iw'
     * Therefore it's possible to generated class conflict(overwrite) with other repl generated
     * class.
     *
     * To prevent generated class name conflict,
     * change prefix of generated class name from each scala compiler (repl) instance.
     *
     * In Spark 2.x, REPL generated wrapper class name should compatible with the pattern
     * ^(\$line(?:\d+)\.\$read)(?:\$\$iw)+$
     *
     * As hashCode() can return a negative integer value and the minus character '-' is invalid
     * in a package name we change it to a numeric value '0' which still conforms to the regexp.
     *
     */
    System.setProperty("scala.repl.name.line", ("$line" + this.hashCode()).replace('-', '0'));
    SESSION_NUM.incrementAndGet();
  }

  public abstract void close() throws InterpreterException;

  public int getProgress(InterpreterContext context) throws InterpreterException {
    return getProgress(Utils.buildJobGroupId(context), context);
  }

  public abstract int getProgress(String jobGroup,
                                  InterpreterContext context) throws InterpreterException;

  public void cancel(InterpreterContext context) throws InterpreterException {
    getSparkContext().cancelJobGroup(Utils.buildJobGroupId(context));
  }

  public Interpreter.FormType getFormType() throws InterpreterException {
    return Interpreter.FormType.SIMPLE;
  }

  public abstract InterpreterResult interpret(String st, InterpreterContext context);

  public abstract InterpreterResult delegateInterpret(KotlinInterpreter kotlinInterpreter,
                                                      String st,
                                                      InterpreterContext context);

  public abstract List<InterpreterCompletion> completion(String buf,
                                                         int cursor,
                                                         InterpreterContext interpreterContext);

  public abstract ClassLoader getScalaShellClassLoader();
}
