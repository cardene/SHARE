Feature: Harvester Scheduling

  # Scenario: Disabled SourceConfig
  #   Given a source "Neat.o" that harvests nightly at 00:30
  #   And source config "Neat.o" is disabled
  #   When harvests are scheduled at 00:30
  #   Then no harvest logs will exist

  # Scenario: Deleted Source
  #   Given a source "Neat.o" that harvests nightly at 00:30
  #   And source "Neat.o" is disabled
  #   When harvests are scheduled at 00:30
  #   Then no harvest logs will exist

  # Scenario: Scheduling disabled SourceConfigs:
  #   Given a source config, neat.o, that harvests daily and is disabled
  #   And the last harvest of neat.o was 2017-01-01
  #   When harvests are scheduled on 2017-01-10
  #   Then neat.o will have 1 harvest logs

  # Scenario: Scheduling disabled Sources:
  #   Given a source config, neat.o, that harvests daily
  #   And the last harvest of neat.o was <PREVIOUS END DATE>
  #   When harvests are scheduled on <DATE>
  #   Then neat.o will have <NUM> harvest logs

  Scenario Outline: Scheduling harvests
    Given a source config, neat.o, that harvests <INTERVAL>
    And the last harvest of neat.o was <PREVIOUS END DATE>
    When harvests are scheduled on <DATE>
    Then neat.o will have <NUM> harvest logs

    Examples:
      | INTERVAL    | PREVIOUS END DATE | DATE       | NUM |
      | daily       | 2017-01-01        | 2017-01-02 | 2   |
      | daily       | 2017-01-01        | 2017-01-03 | 3   |
      | daily       | 2016-01-01        | 2017-01-01 | 367 |
      | weekly      | 2017-01-01        | 2017-01-03 | 1   |
      | weekly      | 2017-01-01        | 2017-01-08 | 2   |
      | weekly      | 2017-01-01        | 2017-01-09 | 2   |
      | monthly     | 2017-01-01        | 2017-01-09 | 1   |
      | monthly     | 2017-01-01        | 2017-02-09 | 2   |
      | monthly     | 2017-01-01        | 2017-03-02 | 3   |
      | fortnightly | 2017-01-01        | 2017-01-15 | 2   |
      | fortnightly | 2016-12-28        | 2017-01-01 | 1   |
      | fortnightly | 2016-12-28        | 2017-02-01 | 3   |
      | yearly      | 2016-02-01        | 2017-02-01 | 2   |
      | yearly      | 2016-02-01        | 2017-01-29 | 1   |

  # We need a new term for backharvest
  Scenario Outline: Automatically scheduling back harvests
    Given a source config, neat.o, that harvests <INTERVAL>
    And neat.o is allowed to be backharvested
    And neat.o's earliest record is <EARLIEST RECORD>
    When harvests are scheduled on 2017-01-01
    Then neat.o will have <NUM> harvest logs

    Examples:
      | INTERVAL | EARLIEST RECORD | NUM  |
      | daily    | 2000-01-01      | 6210 |
      | weekly   | 1990-01-01      | 1408 |
      | monthly  | 2014-05-07      | 32   |
      | yearly   | 2001-01-01      | 16   |

  Scenario Outline: Scheduling first time harvests
    Given a source config, neat.o, that harvests <INTERVAL>
    When harvests are scheduled on 2017-01-01
    Then neat.o will have 1 harvest logs

    Examples:
      | INTERVAL    |
      | daily       |
      | weekly      |
      | fortnightly |
      | yearly      |

  # Scenario: Scheduling harvests with conflicts

  # Scenario: Scheduling updated harvests
