/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.myorg.quickstart;

import org.apache.flink.streaming.api.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.*;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;
import static org.apache.flink.table.api.Expressions.*;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 
 * Skeleton for a Flink DataStream Job.
 *
 * <p>
 * For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink
 * Website</a>.
 *
 * <p>
 * To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>
 * If you change the name of the main class (with the public static void
 * main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for
 * 'mainClass').
 */
public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * env.fromSequence(1, 10);
		 *
		 * then, transform the resulting DataStream<Long> using operations
		 * like
		 * .filter()
		 * .flatMap()
		 * .window()
		 * .process()
		 *
		 * and many more.
		 * Have a look at the programming guide:
		 *
		 * https://nightlies.apache.org/flink/flink-docs-stable/
		 *
		 */

		// Create a DataStream from a list of elements
		DataStream<Tuple2<String, Integer>> stream = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("world", 2));
		// Create table from DataStream
		Table table = tEnv.fromDataStream(stream);
		tEnv.createTemporaryView("table", table);
		Table withConstantColumn = tEnv.from("table").select($("f0"), $("f1"), lit(1).as("MyField"));
		tEnv.createTemporaryView("MyTable", withConstantColumn);
		// Apply TableAggregate Function to Data Tream
		table.printSchema();
		Table result = tEnv.from("MyTable").groupBy($("MyField")).flatAggregate(call(Top2.class, $("f1"))).select($("*"));
		// Convert back to DataStream
		DataStream<Row> row_stream = tEnv.toDataStream(result);
		// print stream
		row_stream.print();
		// Execute program, beginning computation.
		env.execute("Flink Java API Skeleton");
	}

	// mutable accumulator of structured type for the aggregate function
	public static class Top2Accumulator {
		public Integer first;
		public Integer second;
	}

	// function that takes (value INT), stores intermediate results in a structured
	// type of Top2Accumulator, and returns the result as a structured type of
	// Tuple2<Integer, Integer>
	// for value and rank
	public static class Top2 extends TableAggregateFunction<Tuple2<Integer, Integer>, Top2Accumulator> {

		@Override
		public Top2Accumulator createAccumulator() {
			Top2Accumulator acc = new Top2Accumulator();
			acc.first = Integer.MIN_VALUE;
			acc.second = Integer.MIN_VALUE;
			return acc;
		}

		public void accumulate(Top2Accumulator acc, Integer value) {
			if (value > acc.first) {
				acc.second = acc.first;
				acc.first = value;
			} else if (value > acc.second) {
				acc.second = value;
			}
		}

		public void merge(Top2Accumulator acc, Iterable<Top2Accumulator> it) {
			for (Top2Accumulator otherAcc : it) {
				accumulate(acc, otherAcc.first);
				accumulate(acc, otherAcc.second);
			}
		}

		public void emitValue(Top2Accumulator acc, Collector<Tuple2<Integer, Integer>> out) {
			// emit the value and rank
			if (acc.first != Integer.MIN_VALUE) {
				out.collect(Tuple2.of(acc.first, 1));
			}
			if (acc.second != Integer.MIN_VALUE) {
				out.collect(Tuple2.of(acc.second, 2));
			}
		}
	}
}
