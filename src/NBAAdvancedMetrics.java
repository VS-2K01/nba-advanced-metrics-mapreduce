// NBAAdvancedMetrics.java
// A Hadoop MapReduce job that computes custom Player Efficiency Rating (PER),
// Win Shares, and Estimated Plus/Minus (EPM) for every NBA player-team-season
// combination, from raw per-game box score data.
// Written by Vivek Shah.

// Import Statements
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

// NBAAdvancedMetrics.java - Computes the PER, Win Shares, and Estimated Plus/Minus per player per season
// This program will be using the following CSV files:
// games_details.csv
// ranking.csv
// teams.csv

public class NBAAdvancedMetrics {

    private static final String TAG_STATS = "STATS";

    // Class: PlayerStatsMapper
    // Takes games_details.csv and creates KV pairs
    // KEY: PLAYER_ID|TEAM_ID|SEASON
    // VALUE: STATS|PLAYER_NAME|MIN|FGM|FGA|FG3M|FG3A|FTM|FTA|OREB|DREB|REB|AST|STL|BLK|TOV|PF|PTS|PLUS_MINUS
    
    public static class PlayerStatsMapper extends Mapper<Object, Text, Text, Text> {

        // Initialize Output Variables
        private Text outKey = new Text();
        private Text outValue = new Text();

        // Given String of Text Containing Time - Get Minute Value
        private double parseMinutes(String s) {
            try {
                s = s.trim();
                if (s.isEmpty()) return 0.0;
                if (s.contains(":")) {
                    String[] p = s.split(":");
                    return Double.parseDouble(p[0]) + Double.parseDouble(p[1]) / 60.0;
                }
                return Double.parseDouble(s);
            } catch (Exception e) { return 0.0; }
        }

        // Safe parseDouble - try catch exception - give default value of 0.0
        private double parseDouble(String s) {
            try { return Double.parseDouble(s.trim()); }
            catch (Exception e) { return 0.0; }
        }

        // Pulls a line from games_details.csv and calculate KV pair values to create the concatenated string that is the key and value
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // Trim Line - Retun nothing if line is empty or is column headers
            String line = value.toString().trim();
            if (line.startsWith("GAME_ID") || line.isEmpty()) return;
            
            // Splits line by comma - CSV
            String[] cols = line.split(",", -1);
            if (cols.length < 29) return;

            try {
                // Index split line to get individual fields / values
                String gameId = cols[0].trim();
                String teamId = cols[1].trim();
                String playerId = cols[4].trim();
                String playerName = cols[5].trim();

                if (playerId.isEmpty() || teamId.isEmpty() || gameId.length() < 3) return;

                // Characters 1 & 2 (Index 0) represent the last two digits of the year. 
                // For example:
                // "22200477" -> "20" + "22" = "2022"
                // "10300001" -> "20" + "03" = "2003"
                String season = "20" + gameId.substring(1, 3);

                double min = parseMinutes(cols[9]);
                if (min <= 0) return;

                // Parse All Metrics From Split Line
                double fgm = parseDouble(cols[10]);
                double fga = parseDouble(cols[11]);
                double fg3m = parseDouble(cols[13]);
                double fg3a = parseDouble(cols[14]);
                double ftm = parseDouble(cols[16]);
                double fta = parseDouble(cols[17]);
                double oreb = parseDouble(cols[19]);
                double dreb = parseDouble(cols[20]);
                double reb = parseDouble(cols[21]);
                double ast = parseDouble(cols[22]);
                double stl = parseDouble(cols[23]);
                double blk = parseDouble(cols[24]);
                double tov = parseDouble(cols[25]);
                double pf = parseDouble(cols[26]);
                double pts = parseDouble(cols[27]);
                double plusMinus = parseDouble(cols[28]);

                // Create Key
                outKey.set(playerId + "|" + teamId + "|" + season);
                // Create Value
                outValue.set(TAG_STATS + "|" + playerName + "|" + min  + "|" + fgm  + "|" + fga  + "|" + fg3m + "|" + fg3a + "|" + ftm  + "|" + fta  + "|" + oreb + "|" + dreb + "|" + reb  + "|" + ast  + "|" + stl  + "|" + blk  + "|" + tov  + "|" + pf   + "|" + pts  + "|" + plusMinus);
                
                // Output KV Pair
                context.write(outKey, outValue);

            } catch (Exception e) { /* Skips any rows with bad data */ }
        }
    }

   
    // Class: MetricsReducer
    // Reads teams.csv and ranking.csv
    // For each key generated from mapper:
        // Aggregates all game stats for that player/team/season
        // Looks up team abbreviation from teamAbbrev
        // Looks up season win total from teamWins
        // Computes and outputs PER, WS, EPM
    // Advanced Metrics Formulas:
        // PER = (1/MIN) * [PTS + 0.4*FGM - 0.7*FGA - 0.4*(FTA-FTM) + 0.7*OREB + 0.3*DREB + STL + 0.7*AST + 0.7*BLK - 0.4*PF - TOV] * 15
        // WIN_SHARES = (PTS+REB+AST+STL+BLK-TOV-(FGA-FGM)-(FTA-FTM)) / 30 - capped at team season win total
        // EPM = (PLUS_MINUS / MIN) * 36
    public static class MetricsReducer extends Reducer<Text, Text, Text, Text> {
        // Initialize Output Variable
        private Text outValue = new Text();

        // League-Level normalization constants used in advanced metrics
        private static final double LEAGUE_PACE_FACTOR = 15.0;
        private static final double MARGINAL_PTS_PER_WIN = 30.0;
        private static final double EPM_MINS_FACTOR = 36.0;

        // Lookup tables for team abbreviation and wins per season
        private Map<String, String> teamAbbrev = new HashMap<>();
        private Map<String, Double> teamWins = new HashMap<>();

        // Safe parseDouble - try catch exception - give default value of 0.0
        private double parseDouble(String s) {
            try { return Double.parseDouble(s.trim()); }
            catch (Exception e) { return 0.0; }
        }


        // Method: setup
        // Runs once per Reducer task before reduce() is called
        // Reads teams.csv and ranking.csv for abbreviations & wins
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            // Load teams.csv — TEAM_ID -> abbreviation
            try (BufferedReader br = new BufferedReader(new FileReader("teams.csv"))) {
                String line;
                // Reads teams.csv line by line - if it is a valid line of data it extracts team id and abbreviation and 
                // stores it in teamAbbrev
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.startsWith("LEAGUE_ID") || line.isEmpty()) continue;
                    String[] cols = line.split(",", -1);
                    if (cols.length < 6) continue;
                    String tid = cols[1].trim();
                    String abbrev = cols[4].trim();
                    if (!tid.isEmpty() && !abbrev.isEmpty())
                        teamAbbrev.put(tid, abbrev);
                }
                System.err.println("Loaded " + teamAbbrev.size() + " teams.");
            } catch (Exception e) {
                System.err.println("ERROR loading teams.csv: " + e.getMessage());
            }

            // Load ranking.csv — TEAM_ID|SEASON -> max wins
            try (BufferedReader br = new BufferedReader(new FileReader("ranking.csv"))) {
                String line;
                // Reads ranking.csv line by line - if it is a valid line of data it extracts team id and season id and # of wins as a 
                // string and stores it in teamWins
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.startsWith("TEAM_ID") || line.isEmpty()) continue;
                    String[] cols = line.split(",", -1);
                    if (cols.length < 10) continue;

                    String tid = cols[0].trim();
                    String seasonId = cols[2].trim();
                    String winsStr = cols[7].trim();

                    if (tid.isEmpty() || seasonId.length() < 4) continue;

                    String year = seasonId.substring(seasonId.length() - 4);
                    double wins = parseDouble(winsStr);

                    String wKey = tid + "|" + year;
                    // If team id | season not in teamsWins add it, if it is and the currents wins is higher than the stored wins, replace it
                    if (!teamWins.containsKey(wKey) || wins > teamWins.get(wKey))
                        teamWins.put(wKey, wins);
                }
                System.err.println("Loaded " + teamWins.size() + " team-season win totals.");
            } catch (Exception e) {
                System.err.println("ERROR loading ranking.csv: " + e.getMessage());
            }
        }

        // Method: reduce
        // Using KV pairs from mapper - aggregate per-game stats and compute PER, Win Shares, and EPM per player per season
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // Read Key and Split by | - Format: PLAYER_ID|TEAM_ID|SEASON
            String keyStr = key.toString();
            String[] parts = keyStr.split("\\|", 3);
            if (parts.length < 3) return;

            // Store Key Parts into Variables
            String playerId = parts[0];
            String teamId = parts[1];
            String season = parts[2];

            // Initialize Variables to Store Season Totals & Player Name (Player_<playerId>)
            double totalMin = 0, totalFGM = 0, totalFGA = 0;
            double totalFG3M = 0, totalFG3A = 0, totalFTM = 0, totalFTA = 0;
            double totalOREB = 0, totalDREB = 0, totalREB = 0, totalAST = 0;
            double totalSTL = 0, totalBLK = 0, totalTOV = 0, totalPF = 0;
            double totalPTS = 0, totalPlusMinus = 0;
            double gamesPlayed = 0;
            String playerName = "Player_" + playerId;
            // Loops through each value (game total/record) for each key
            for (Text val : values) {
                // Convert Value to String - Split & Extract Values Into Running Total
                String v = val.toString();
                String[] p = v.split("\\|", -1);
                if (p.length < 19 || !p[0].equals(TAG_STATS)) continue;

                if (!p[1].isEmpty()) playerName = p[1];

                totalMin += parseDouble(p[2]);
                totalFGM += parseDouble(p[3]);
                totalFGA += parseDouble(p[4]);
                totalFG3M += parseDouble(p[5]);
                totalFG3A += parseDouble(p[6]);
                totalFTM += parseDouble(p[7]);
                totalFTA += parseDouble(p[8]);
                totalOREB += parseDouble(p[9]);
                totalDREB += parseDouble(p[10]);
                totalREB += parseDouble(p[11]);
                totalAST += parseDouble(p[12]);
                totalSTL += parseDouble(p[13]);
                totalBLK += parseDouble(p[14]);
                totalTOV += parseDouble(p[15]);
                totalPF += parseDouble(p[16]);
                totalPTS += parseDouble(p[17]);
                totalPlusMinus += parseDouble(p[18]);
                gamesPlayed++;
            }

            if (totalMin <= 0 || gamesPlayed == 0) return;

            // Pull team abbreviation and wins from HashMaps populated in setup()
            String displayTeam = teamAbbrev.getOrDefault(teamId, teamId);
            double wins = teamWins.getOrDefault(teamId + "|" + season, 0.0);

            // Advanced Metric 1: Player Efficiency Rating (PER)
            // Weighted per-minute production normalized to league average of 15
            double uPER = (1.0 / totalMin) * (totalPTS + (0.4 * totalFGM) - (0.7 * totalFGA) - (0.4 * (totalFTA - totalFTM)) + (0.7 * totalOREB) + (0.3 * totalDREB) + totalSTL + (0.7 * totalAST) + (0.7 * totalBLK) - (0.4 * totalPF) - totalTOV);
            // Scale raw PER with NBA league average of 15
            double per = uPER * LEAGUE_PACE_FACTOR;

            // Advanced Metric 2: Win Shares (WS)
            // Net positive contributions / marginal points per win (~30)
            double netContrib = totalPTS + totalREB + totalAST + totalSTL + totalBLK - totalTOV - (totalFGA - totalFGM) - (totalFTA - totalFTM);
            double winShares  = netContrib / MARGINAL_PTS_PER_WIN;
            if (wins > 0 && winShares > wins) winShares = wins;
            if (winShares < 0) winShares = 0;

            // Advanced Metric 3: Estimated Plus/Minus (EPM)
            // Raw plus/minus normalized to per-36-minute rate
            double epm = (totalPlusMinus / totalMin) * EPM_MINS_FACTOR;

            // Format and generate output
            String outputKey = playerId + "|" + teamId + "|" + season;
            String output = String.format(
                "SEASON: %s | PLAYER: %-25s | TEAM: %-5s | GP: %3.0f | MIN: %6.1f | PER: %6.2f | WIN_SHARES: %5.2f | EPM: %+6.2f",
                season, playerName, displayTeam, gamesPlayed, totalMin, per, winShares, epm
            );
            
            // Output Key - Value Created Above
            outValue.set(output);
            context.write(new Text(outputKey), outValue);
        }
    }

    // Main Class
    // Expected Arguments - games_details.csv file path, output file path
    public static void main(String[] args) throws Exception {
        
        // Less than 2 arguments - end execution and output help
        if (args.length < 2) {
            System.err.println("Usage: NBAAdvancedMetrics <games_details> <output>");
            System.exit(1);
        }

        // Initialize Hadoop Configuration and create new MapReduce job instance
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "NBA Advanced Metrics: PER, Win Shares, EPM");

        // Set the main class for JAR file lookup on the datanodes
        // Set Reduce class and define output key and value data types
        job.setJarByClass(NBAAdvancedMetrics.class);
        job.setReducerClass(MetricsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Add teams.csv and ranking.csv files to DistributedCache for setup()
        job.addCacheFile(new URI("hdfs:///nba/input/teams/teams.csv#teams.csv"));
        job.addCacheFile(new URI("hdfs:///nba/input/ranking/ranking.csv#ranking.csv"));

        // Set the Input Path (Multiple) for the newly created MapReduce job instance to the file path for games_details.csv, 
        // the TextInputFormat class, and the Mapper class
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PlayerStatsMapper.class);
        
        // Set the Output Path for the newly created MapReduce job instance to the output file path passed as an argument
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        // If job completes successfully exit program
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
