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

package org.apache.flink.streaming.examples.async.my;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.streaming.examples.twitter.util.TwitterExampleData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExecutorUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteFuture;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.StringTokenizer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Implements the "TwitterStream" program that computes a most used word
 * occurrence over JSON objects in a streaming fashion.
 *
 * <p>The input is a Tweet stream from a TwitterSource.
 *
 * <p>Usage: <code>Usage: TwitterExample [--output &lt;path&gt;]
 * [--twitter-source.consumerKey &lt;key&gt; --twitter-source.consumerSecret &lt;secret&gt; --twitter-source.token &lt;token&gt; --twitter-source.tokenSecret &lt;tokenSecret&gt;]</code><br>
 *
 * <p>If no parameters are provided, the program is run with default data from
 * {@link TwitterExampleData}.
 *
 * <p>This example shows how to:
 * <ul>
 * <li>acquire external data,
 * <li>use in-line defined functions,
 * <li>handle flattened stream inputs.
 * </ul>
 */
public class TwitterExample2 {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);
		System.out.println("Usage: TwitterExample [--output <path>] " +
				"[--twitter-source.consumerKey <key> --twitter-source.consumerSecret <secret> --twitter-source.token <token> --twitter-source.tokenSecret <tokenSecret>]");

		// set up the execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		env.setParallelism(params.getInt("parallelism", 1));

		// get input data
		DataStream<String> streamSource;
		if (params.has(TwitterSource.CONSUMER_KEY) &&
				params.has(TwitterSource.CONSUMER_SECRET) &&
				params.has(TwitterSource.TOKEN) &&
				params.has(TwitterSource.TOKEN_SECRET)
				) {
			streamSource = env.addSource(new TwitterSource(params.getProperties()));
		} else {
			System.out.println("Executing TwitterStream example with default props.");
			System.out.println("Use --twitter-source.consumerKey <key> --twitter-source.consumerSecret <secret> " +
					"--twitter-source.token <token> --twitter-source.tokenSecret <tokenSecret> specify the authentication info.");
			// get default test text data
			streamSource = env.fromElements(TwitterExampleData.TEXTS);
		}

		DataStream<Tuple2<String, Integer>> tweets = streamSource
				// selecting English tweets and splitting to (word, 1)
				.flatMap(new SelectEnglishAndTokenizeFlatMap())
				// group by words and sum their occurrences
				.keyBy(0).sum(1);

		AsyncClass asyncFunction = new AsyncClass();

		DataStream<Tuple2<String, Integer>> result = AsyncDataStream.unorderedWait(tweets, asyncFunction, 10000L, TimeUnit.MILLISECONDS,20);

		// emit result
		if (params.has("output")) {
			tweets.writeAsText(params.get("output"));
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			result.print();
		}

		// execute program
		env.execute("Twitter Streaming Example");
	}

	public static class AsyncClass extends RichAsyncFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

		Ignite ignite;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			ignite = Ignition.start("C:\\Users\\v_1_v\\Downloads\\1\\apache-ignite-2.8.0-bin\\apache-ignite-2.8.0-bin\\examples\\config\\example-ignite.xml");
		}

		@Override
		public void close() throws Exception {
			super.close();
			ignite.close();
		}

		@Override
		public void asyncInvoke(Tuple2<String, Integer> stringIntegerTuple2, ResultFuture<Tuple2<String, Integer>> resultFuture) throws Exception {
			CompletableFuture.supplyAsync((Supplier<Tuple2<String, Integer>>) () -> {
				System.out.println("");
				return null;
			}).thenAccept((Tuple2<String, Integer> tuple) -> {
				Tuple2<String, Integer> stringIntegerTuple3 = stringIntegerTuple2;
				stringIntegerTuple3.f1 = stringIntegerTuple3.f1 + 10;
				resultFuture.complete(Collections.singleton(stringIntegerTuple3));
			});
		}

		public Object getCache() {
			String CACHE_NAME = "CACHE_NAME";

			CacheConfiguration<Integer, String> cfg = new CacheConfiguration<>();

			cfg.setCacheMode(CacheMode.PARTITIONED);
			cfg.setName(CACHE_NAME);

			// Auto-close cache at the end of the example.
			IgniteCache<Integer, String> cache2 = ignite.getOrCreateCache(cfg);

			try (IgniteCache<Integer, String> cache = ignite.getOrCreateCache(cfg)) {
				// Enable asynchronous mode.
				IgniteCache<Integer, String> asyncCache = cache.withAsync();

				Collection<IgniteFuture<?>> futs = new ArrayList<>();

				// Execute several puts asynchronously.
				for (int i = 0; i < 10; i++) {
					asyncCache.put(i, String.valueOf(i));

					futs.add(asyncCache.future());
				}

				// Wait for completion of all futures.
				futs.forEach(IgniteFuture::get);

				// Execute get operation asynchronously.
				asyncCache.get(1);

				// Asynchronously wait for result.
				asyncCache.<String>future().listen(fut ->
					System.out.println("Get operation completed [value=" + fut.get() + ']'));

				IgniteFuture<String> async = cache.getAsync(2);
				async.listen(fut -> {
					System.out.println("Get new async operation completed [value=" + fut.get() + ']');
				});
			} finally {
				// Distributed cache could be removed from cluster only by #destroyCache() call.
				ignite.destroyCache(CACHE_NAME);
			}
			return null;
		}

	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	/**
	 * Deserialize JSON from twitter source
	 *
	 * <p>Implements a string tokenizer that splits sentences into words as a
	 * user-defined FlatMapFunction. The function takes a line (String) and
	 * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
	 * Integer>}).
	 */
	public static class SelectEnglishAndTokenizeFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		private transient ObjectMapper jsonParser;

		/**
		 * Select the language from the incoming JSON text.
		 */
		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
			if (jsonParser == null) {
				jsonParser = new ObjectMapper();
			}
			JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
			boolean isEnglish = jsonNode.has("user") && jsonNode.get("user").has("lang") && jsonNode.get("user").get("lang").asText().equals("en");
			boolean hasText = jsonNode.has("text");
			if (isEnglish && hasText) {
				// message of tweet
				StringTokenizer tokenizer = new StringTokenizer(jsonNode.get("text").asText());

				// split the message
				while (tokenizer.hasMoreTokens()) {
					String result = tokenizer.nextToken().replaceAll("\\s*", "").toLowerCase();

					if (!result.equals("")) {
						out.collect(new Tuple2<>(result, 1));
					}
				}
			}
		}
	}

}
