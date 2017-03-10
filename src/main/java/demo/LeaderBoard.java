package demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;

/**
 * This class is the third in a series of four pipelines that tell a story in a 'gaming' domain,
 * following {@link UserScore} and {@link HourlyTeamScore}. Concepts include: processing unbounded
 * data using fixed windows; use of custom timestamps and event-time processing; generation of
 * early/speculative results; using .accumulatingFiredPanes() to do cumulative processing of late-
 * arriving data.
 *
 * <p>This pipeline processes an unbounded stream of 'game events'. The calculation of the team
 * scores uses fixed windowing based on event time (the time of the game play event), not
 * processing time (the time that an event is processed by the pipeline). The pipeline calculates
 * the sum of scores per team, for each window. By default, the team scores are calculated using
 * one-hour windows.
 *
 * <p>In contrast-- to demo another windowing option-- the user scores are calculated using a
 * global window, which periodically (every ten minutes) emits cumulative user score sums.
 *
 * <p>In contrast to the previous pipelines in the series, which used static, finite input data,
 * here we're using an unbounded data source, which lets us provide speculative results, and allows
 * handling of late data, at much lower latency. We can use the early/speculative results to keep a
 * 'leaderboard' updated in near-realtime. Our handling of late data lets us generate correct
 * results, e.g. for 'team prizes'. We're now outputting window results as they're
 * calculated, giving us much lower latency than with the previous batch examples.
 *
 * <p>Run {@code injector.Injector} to generate pubsub data for this pipeline.  The Injector
 * documentation provides more detail on how to do this.
 *
 * <p>To execute this pipeline using the Dataflow service, specify the pipeline configuration
 * like this:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 *   --tempLocation=gs://YOUR_TEMP_DIRECTORY
 *   --runner=BlockingDataflowRunner
 *   --dataset=YOUR-DATASET
 *   --topic=projects/YOUR-PROJECT/topics/YOUR-TOPIC
 * }
 * </pre>
 * where the BigQuery dataset you specify must already exist.
 * The PubSub topic you specify should be the same topic to which the Injector is publishing.
 */
public class LeaderBoard extends HourlyTeamScore {

  private static final String TIMESTAMP_ATTRIBUTE = "timestamp_ms";
  static final Duration TWO_MINUTES = Duration.standardMinutes(2);
  static final Duration FIVE_MINUTES = Duration.standardMinutes(5);
  static final Duration TEN_MINUTES = Duration.standardMinutes(10);


  interface Options extends HourlyTeamScore.Options, StreamingOptions {
    @Description("Pub/Sub topic to read from")
    @Default.String("projects/apache-beam-demo/topics/game-fjp")
    String getTopic();
    void setTopic(String value);
  }

  public static void main(String[] args) throws Exception {

    Options options = 
    		PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    options.setStreaming(true);
    Pipeline pipeline = Pipeline.create(options);

    pipeline
	    .apply(PubsubIO.<String>read()
	        .timestampLabel(TIMESTAMP_ATTRIBUTE).topic(options.getTopic())
	        .withCoder(StringUtf8Coder.of()))
	    .apply("ParseGameEvent", ParDo.of(new ParseEventFn()))

      	.apply("FixedWindows", Window.<GameActionInfo>into(FixedWindows.of(FIVE_MINUTES))
	            .triggering(AfterWatermark.pastEndOfWindow()
	        		.withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
	        				.plusDelayOf(TWO_MINUTES))
	                .withLateFirings(AfterPane.elementCountAtLeast(1)))
	        .withAllowedLateness(TEN_MINUTES)
	        .accumulatingFiredPanes())
   		  
         .apply("ExtractTeamScore", new CalculateTeamScores(options.getOutputPrefix()));
    
    pipeline.run();
  }


 
}
