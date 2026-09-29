# Architecture Deep Dive

How the MapReduce job is structured, why the design choices were made, and the real problems that came up running it on a live 4-node Hadoop cluster.

## Pipeline overview

```
games_details.csv (93M, ~668k rows)
        │
        ▼
┌───────────────────┐
│ PlayerStatsMapper  │  parses one box-score line → emits
│                    │  KEY:   PLAYER_ID|TEAM_ID|SEASON
│                    │  VALUE: STATS|PLAYER_NAME|MIN|FGM|FGA|...|PLUS_MINUS
└─────────┬──────────┘
          │  shuffle & sort (grouped by composite key)
          ▼
┌───────────────────┐
│  MetricsReducer    │  setup(): loads teams.csv + ranking.csv
│                    │           from DistributedCache into memory
│                    │  reduce(): aggregates every game for one
│                    │            player-team-season, computes
│                    │            PER, Win Shares, EPM
└─────────┬──────────┘
          ▼
nba_results.txt — one line per player-team-season
```

## Why a composite key

A player can play for more than one team in a season (a trade), and the same `PLAYER_ID` shows up across many seasons. Keying on `PLAYER_ID` alone would silently merge stats across teams and years into one meaningless total. The key used is:

```
PLAYER_ID|TEAM_ID|SEASON
```

which guarantees every group the reducer sees is exactly one player's games for one team in one season — the correct unit for a per-season advanced-stat calculation.

## Getting lookup data to the reducer: DistributedCache

`teams.csv` (team ID → abbreviation) and `ranking.csv` (team ID → season win total, used to cap Win Shares) are small reference tables needed inside `MetricsReducer.reduce()`. Rather than joining them through the MapReduce shuffle, they're distributed to every node via Hadoop's `DistributedCache`:

```java
job.addCacheFile(new URI("hdfs:///nba/input/teams.csv#teams.csv"));
job.addCacheFile(new URI("hdfs:///nba/input/ranking.csv#ranking.csv"));
```

and read once per reducer task in `setup()` into in-memory `HashMap`s (`teamAbbrev`, `teamWins`), rather than in every call to `reduce()`. This is the standard pattern for a large dataset joined against a small one — the small side is broadcast instead of shuffled.

## The three metrics (custom implementations, not the official NBA formulas)

- **PER (Player Efficiency Rating)** — `uPER * 15.0`, where `uPER` is a per-minute weighted sum of the box-score stats (points, rebounds, assists, steals, blocks, turnovers, etc.), each with a fixed weight, normalized by minutes played. The multiplier is chosen so a league-average performance lands near 15, in the spirit of the real PER, but the weights are hand-picked here, not derived from the league-wide regression the NBA's actual formula uses.
- **Win Shares** — `netContrib / 30.0`, where `netContrib` is a per-player estimate of statistical contribution over the season, then capped at the team's actual win total for that season (from `ranking.csv`) and floored at 0, so no player is ever credited with more wins than the team had or a negative number of wins.
- **EPM (Estimated Plus/Minus)** — `(totalPlusMinus / totalMin) * 36.0`: the player's box-score plus/minus per minute, scaled to a 36-minute game, as a rough estimate of per-game impact.

All three are intentionally simplified, from-scratch approximations of real basketball analytics metrics — good for demonstrating the MapReduce aggregation pattern, not for replacing the NBA's own advanced-stats pipeline. See [Future improvements](#future-improvements) below for what a more rigorous version would need.

## Environment

- 4-node Hadoop cluster on AWS EC2 (`ds644-Master` + `ds644-Slave1/2/3`), Hadoop 3.3.6, OpenJDK 1.8.0_482
- Master runs NameNode + ResourceManager; each slave runs a DataNode + NodeManager
- Compiled and run with:
  ```bash
  export HADOOP_CLASSPATH=$(hadoop classpath)
  javac -classpath $HADOOP_CLASSPATH -d . NBAAdvancedMetrics.java
  jar -cvf NBAAdvancedMetrics.jar *.class
  hdfs dfsadmin -safemode leave
  hdfs dfs -rm -r /nba/output
  hadoop jar NBAAdvancedMetrics.jar NBAAdvancedMetrics /nba/input/games_details /nba/output
  ```

## Job output stats (real run)

| Stage | Records |
|---|---|
| Map input | 668,629 |
| Map output | 558,746 |
| Reduce output (unique player-team-season rows) | 12,742 |

HDFS bytes read: ~93,079,767 (the full `games_details.csv`).

## Challenges and fixes

**1. Node instability on `t3.micro`.** The cluster was first provisioned on `t3.micro` instances. Under load, DataNode and NodeManager processes would intermittently drop out, destabilizing the cluster mid-job. Upgrading all four nodes to `t3.medium` (more RAM) resolved it — Hadoop's per-daemon memory footprint doesn't fit comfortably in a `t3.micro`'s 1 GB.

**2. A join that looked right but produced garbage.** An early version tried to bring in player names by joining on `TEAM_ID` between `games_details.csv` and a separate players/teams table. The `TEAM_ID` values didn't line up consistently between files, so the join silently produced wrong pairings. The fix was to stop joining for the player name at all — `games_details.csv` already carries the player's name on every row, so the mapper reads it directly from the same record it's already parsing, removing the bad join entirely.

**3. Empty reducer output from a DistributedCache/shuffle ordering assumption.** An earlier design loaded the lookup files as regular MapReduce input alongside `games_details.csv`, assuming Hadoop's shuffle would present them to the reducer in a useful order relative to the stat records. Hadoop sorts keys alphabetically/numerically, not by "which file a record came from," so the lookup rows and stat rows never lined up the way that design expected, and the reducer produced no output. Moving `teams.csv` and `ranking.csv` into the `DistributedCache` (see above) sidesteps the ordering issue entirely — the reducer has the full lookup table in memory before it processes a single stat record, independent of shuffle order.

## Future improvements

- Replace the hand-picked PER weights with the NBA's actual published PER formula (a league-average-normalized regression), for a metric comparable to public sources.
- Implement Win Shares using the real methodology (offensive/defensive win shares split, possession-based), rather than the linear approximation used here.
- Derive EPM from an actual regression model (e.g. ridge regression on lineup-level plus/minus) instead of a fixed per-minute scaling.
- Add a `Combiner` class to reduce shuffle volume — with 668k map outputs funneling into 12.7k groups, a combiner that pre-aggregates per-mapper would cut network I/O meaningfully.
- Port the job to Apache Spark, which would let the same aggregation run as an in-memory DataFrame pipeline instead of a disk-backed shuffle, and would remove the DistributedCache workaround entirely (broadcast variables replace it directly).
