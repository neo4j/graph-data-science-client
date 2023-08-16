#!/usr/bin/env ruby
# frozen_string_literal: true

require 'open3'
require 'asciidoctor'
require 'minitest/autorun'

INIT_GDS = '
import os

import pandas

from graphdatascience import GraphDataScience
from graphdatascience.server_version.server_version import ServerVersion

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
URI_TLS = os.environ.get("NEO4J_URI", "bolt+ssc://localhost:7687")

NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"
if os.environ.get("NEO4J_USER"):
    NEO4J_USER = os.environ.get("NEO4J_USER", "DUMMY")
    NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "neo4j")

gds = GraphDataScience(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
gds.set_database("neo4j")
'

CLEAN_UP = '
finally:
    res = gds.graph.list()
    for graph_name in res["graphName"]:
        gds.graph.get(graph_name).drop(failIfMissing=True)
    res = gds.pipeline.list()
    for pipeline_name in res["pipelineName"]:
        gds.pipeline.get(pipeline_name).drop(failIfMissing=True)
    res = gds.beta.model.list()
    for model_info in res["modelInfo"]:
        model = gds.model.get(model_info["modelName"])
        if (model.stored()):
            gds.alpha.model.delete(model)
        if (model.exists()):
            model.drop(failIfMissing=True)
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

def complete_raw_scripts(raw_scripts)
  raw_scripts.map do |s|
    indented_s = "try:\n"
    s.each_line do |line|
      indented_s += "    #{line}"
    end
    INIT_GDS + indented_s + CLEAN_UP
  end
end

def block_to_raw_code(block)
  if block.attr?('min-server-version')
    min_gds_version = block.attr('min-server-version')
    raw_code = "if ServerVersion.from_string(\"#{min_gds_version}\") <= ServerVersion.from_string(gds.version()):\n"
    block.source.each_line { |line| raw_code += "    #{line}" }
    raw_code
  else
    block.source
  end
end

def filter_source_blocks(source_blocks, scope)
  testable_source_blocks = source_blocks.select { |b| !b.has_role?('no-test') && b.attr('language') == 'python' }
  testable_source_blocks = testable_source_blocks.reject { |b| b.attr? 'enterprise' } unless scope == :enterprise

  if scope == :networkx
    testable_source_blocks.select { |b| b.attr? 'networkx' }
  else
    testable_source_blocks.reject { |b| b.attr? 'networkx' }
  end
end

def scripts_of_file(path, scope)
  doc = Asciidoctor.load_file path, safe: :safe

  source_blocks = doc.find_by style: 'source'
  testable_source_blocks = filter_source_blocks(source_blocks, scope)

  raw_scripts = []
  raw_scripts_by_group = Hash.new { |h, k| h[k] = "# #{k}" }

  testable_source_blocks.each do |b|
    if b.attr? 'group'
      group = b.attr 'group'
      raw_scripts_by_group[group] += "\n#{block_to_raw_code(b)}"
    else
      raw_scripts.push(block_to_raw_code(b))
    end
  end

  raw_scripts_by_group.each_value do |s|
    raw_scripts.push(s)
  end

  complete_raw_scripts(raw_scripts)
end

class DocTest < Minitest::Test
  def run_doc_scripts(scope)
    files = doc_files

    files.each do |f|
      scripts = scripts_of_file(f, scope)

      scripts.each do |s|
        stdout, stderr, status = Open3.capture3 "#{ARGV[0]} -c '#{s}'"
        assert status == 0,
               "A doc test of file '#{f}' failed:\n\nTest script: #{s}\nstdout: #{stdout}\nstderr: #{stderr}"
      end
    end
  end

  def test_community
    run_doc_scripts(:community)
  end

  def test_enterprise
    run_doc_scripts(:enterprise)
  end

  def test_networkx
    run_doc_scripts(:networkx)
  end
end
