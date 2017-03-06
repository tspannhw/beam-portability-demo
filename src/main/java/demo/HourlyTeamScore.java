package demo;

import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.TimeZone;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.IOChannelFactory;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;


/**
 * 
 */
public class HourlyTeamScore {

  static final Duration ONE_HOUR = Duration.standardMinutes(60);


  public interface Options extends PipelineOptions {

    @Description("Path to the data file(s) containing game data.")
    // The default maps to two large Google Cloud Storage files (each ~12GB) holding two subsequent
    // day's worth (roughly) of data.
    @Default.String("gs://apache-beam-samples/game/gaming_data*.csv")
    String getInput();
    void setInput(String value);
    
    @Description("Output file prefix")
    @Validation.Required
    String getOutputPrefix();
    void setOutputPrefix(String value);
  }
  
  

  /**
   * Class to hold info about a game event.
   */
  @DefaultCoder(AvroCoder.class)
  static class GameActionInfo {
    @Nullable String user;
    @Nullable String team;
    @Nullable Integer score;
    @Nullable Long timestamp;

    public GameActionInfo() {}

    public GameActionInfo(String user, String team, Integer score, Long timestamp) {
      this.user = user;
      this.team = team;
      this.score = score;
      this.timestamp = timestamp;
    }

    public String getUser() {
      return this.user;
    }
    public String getTeam() {
      return this.team;
    }
    public Integer getScore() {
      return this.score;
    }
    public Long getTimestamp() {
      return this.timestamp;
    }
  }
  
  public static class WriteWindowedFilesDoFn
	  extends DoFn<KV<IntervalWindow, Iterable<KV<String, Integer>>>, Void> {
	
    final byte[] NEWLINE = "\n".getBytes(StandardCharsets.UTF_8);
	final Coder<String> STRING_CODER = StringUtf8Coder.of();
	
	private static DateTimeFormatter formatter = ISODateTimeFormat.hourMinute();
	
	private final String output;
	
	public WriteWindowedFilesDoFn(String output) {
	  this.output = output;
	}
	
	public String fileForWindow(String output, ProcessContext context) {
	  IntervalWindow window = context.element().getKey();
	  String fileName = String.format(
	      "%s-%s-%s", output, formatter.print(window.start()), formatter.print(window.end()));
	  
	  if (context.pane().getTiming().equals(PaneInfo.Timing.EARLY)) {
		  fileName += String.format("_early-%s", formatter.print(context.timestamp()));
	  } else if (context.pane().getTiming().equals(PaneInfo.Timing.LATE)) {
		  fileName += String.format("_late-%s", formatter.print(context.timestamp()));
	  }
	  
	  return fileName;
	}
	
	@ProcessElement
	public void processElement(ProcessContext context) throws Exception {
	  // Build a file name from the window
	  
	  String outputShard = fileForWindow(output, context);
	
	  // Open the file and write all the values
	  IOChannelFactory factory = IOChannelUtils.getFactory(outputShard);
	  OutputStream out = Channels.newOutputStream(factory.create(outputShard, "text/plain"));
	  for (KV<String, Integer> wordCount : context.element().getValue()) {
	    STRING_CODER.encode(
	        wordCount.getKey() + ": " + wordCount.getValue(), out, Coder.Context.OUTER);
	    out.write(NEWLINE);
	  }
	  out.close();
	}
  }

  public static class ExtractAndSumScore
		extends PTransform<PCollection<GameActionInfo>, PCollection<Void>> {
		
	String filepath;
	
	ExtractAndSumScore(String filepath) {
		this.filepath = filepath;
	}
	
	@Override
	public PCollection<Void> expand(PCollection<GameActionInfo> gameInfo) {
	
	  return gameInfo
	    .apply(MapElements
	        .via((GameActionInfo gInfo) -> KV.of(gInfo.getTeam(), gInfo.getScore()))
	        .withOutputType(
	            TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers())))
	    .apply(Sum.<String>integersPerKey())
		.apply(ParDo.of(new DoFn<KV<String, Integer>, KV<IntervalWindow, KV<String, Integer>>>() {
		                  @ProcessElement
		                  public void processElement(ProcessContext context, IntervalWindow window) {
		                    context.output(KV.of(window, context.element()));
		                  }
		                }))
        .apply(GroupByKey.<IntervalWindow, KV<String, Integer>>create())
        .apply(ParDo.of(new WriteWindowedFilesDoFn(filepath)));
	}
  }


  /**
   * Parses the raw game event info into GameActionInfo objects. Each event line has the following
   * format: username,teamname,score,timestamp_in_ms,readable_time
   * e.g.:
   * user2_AsparagusPig,AsparagusPig,10,1445230923951,2015-11-02 09:09:28.224
   * The human-readable time string is not used here.
   */
  static class ParseEventFn extends DoFn<String, GameActionInfo> {

    // Log and count parse errors.
    private static final Logger LOG = LoggerFactory.getLogger(ParseEventFn.class);
    private final Aggregator<Long, Long> numParseErrors =
        createAggregator("ParseErrors", Sum.ofLongs());

    @ProcessElement
    public void processElement(ProcessContext c) {
      String[] components = c.element().split(",");
      try {
        String user = components[0].trim();
        String team = components[1].trim();
        Integer score = Integer.parseInt(components[2].trim());
        Long timestamp = Long.parseLong(components[3].trim());
        GameActionInfo gInfo = new GameActionInfo(user, team, score, timestamp);
        c.output(gInfo);
      } catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
        numParseErrors.addValue(1L);
        LOG.info("Parse error on " + c.element() + ", " + e.getMessage());
      }
    }
  }
  
  
  /**
   * Run a batch pipeline to do windowed analysis of the data.
   */
  public static void main(String[] args) throws Exception {
    // Begin constructing a pipeline configured by commandline flags.
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline pipeline = Pipeline.create(options);


    // Read 'gaming' events from a text file.
    pipeline
    	.apply(TextIO.Read.from(options.getInput()))
      	.apply("ParseGameEvent", ParDo.of(new ParseEventFn()))
      	.apply("AddEventTimestamps", WithTimestamps.of((GameActionInfo i) -> new Instant(i.getTimestamp())))
      
      	.apply("FixedWindows", Window.<GameActionInfo>into(FixedWindows.of(ONE_HOUR)))

      	.apply("SumTeamScores", new ExtractAndSumScore(options.getOutputPrefix()));

    pipeline.run();
  }
}
