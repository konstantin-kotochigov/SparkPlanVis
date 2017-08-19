# SparkPlanVis

SparkPlanVis is a cute plan visualizer for Apache Spark physical plans

[ScreenShot](./screen.jpg)

## Description

It uses generated with df.explain() string as a source, parses its structure based on lines indentation and generates html web page with interactive javascript visualization.

[D3.js](http://d3js.org) javascript library is used as an engine for data visualization. This [implementation](https://bl.ocks.org/mbostock/4339083) of a collapsible tree is used as a base. 

## ToDOs
- Extend functionality for other types of Spark plans:
  - Parsed Logical
  - Analyzed Logical
  - Optimized Logical
- Fix the bug with Sort-based nodes (SortMerge, SortAggregate etc) since Spark does not indent them properly
- Embed JSON representation in html itself rather then loading it as a separate file
- Display full plan lines properly