#!/usr/bin/env ruby
# frozen_string_literal: true

require 'open3'
require 'asciidoctor'
require 'minitest/autorun'

INIT_GDS = '
import os

import pandas

from graphdatascience import GraphDataScience

URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
URI_TLS = os.environ.get("NEO4J_URI", "bolt+ssc://localhost:7687")

AUTH = ("neo4j", "password")
if os.environ.get("NEO4J_USER"):
    AUTH = (
        os.environ.get("NEO4J_USER", "DUMMY"),
        os.environ.get("NEO4J_PASSWORD", "neo4j"),
    )

gds = GraphDataScience(URI, auth=AUTH)
'

CLEAN_UP = '
finally:
    res = gds.graph.list()
    for graph_name in res["graphName"]:
        gds.graph.drop(gds.graph.get(graph_name), failIfMissing=True)
    gds.run_cypher("MATCH (n) DETACH DELETE (n)")
'

def doc_files
  Dir["#{__dir__}/../modules/ROOT/pages/**/*.adoc"]
end

def add_to_group(scripts_by_group, block)
  group = block.attr 'group'
  source = block.source
  if scripts_by_group[group].nil?
    scripts_by_group[group] = source
  else
    scripts_by_group[group] += "\n#{source}"
  end
end

def scripts_of_file(path)
  doc = Asciidoctor.load_file path, safe: :safe

  source_blocks = doc.find_by style: 'source'
  testable_source_blocks = source_blocks.select { |b| b.has_role? 'test' }

  raw_scripts = []
  raw_scripts_by_group = {}

  testable_source_blocks.each do |b|
    if b.attr? 'group'
      add_to_group(raw_scripts_by_group, b)
    else
      raw_scripts.push(b.source)
    end
  end

  raw_scripts_by_group.each_value do |s|
    raw_scripts.push(s)
  end

  raw_scripts.map do |s|
    indented_s = "try:\n"
    s.each_line do |line|
      indented_s += "    #{line}"
    end
    INIT_GDS + indented_s + CLEAN_UP
  end
end

class DocTest < Minitest::Test
  def test_doc_scripts
    files = doc_files

    files.each do |f|
      scripts = scripts_of_file(f)

      scripts.each do |s|
        stdout, stderr, status = Open3.capture3 "#{ARGV[0]} -c '#{s}'"
        assert status == 0, "A doc test of file '#{f}' failed:\n\nTest script: #{s}\nstdout: #{stdout}\nstderr: #{stderr}"
      end
    end
  end
end
