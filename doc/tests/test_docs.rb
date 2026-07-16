#!/usr/bin/env ruby
# frozen_string_literal: true

require 'logger'
require 'open3'
require 'asciidoctor'
require 'minitest/autorun'

# Progress logging goes to stderr so it interleaves with (but does not corrupt) the
# Minitest report on stdout. Set DOC_TEST_LOGLEVEL=DEBUG for per-script logging.
LOGGER = Logger.new($stderr)
LOGGER.level = ENV.fetch('DOC_TEST_LOGLEVEL', 'INFO')
LOGGER.formatter = proc do |severity, datetime, _progname, msg|
  "#{datetime.strftime('%Y-%m-%d %H:%M:%S')} #{severity.ljust(5)} #{msg}\n"
end

# Boilerplate prepended to every doc snippet: connect a GraphDataScience object to the
# plugin/self-managed Neo4j database configured via the NEO4J_* env vars.
INIT_GDS = '
import os

import pandas

from graphdatascience import GraphDataScience, ServerVersion

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
URI_TLS = os.environ.get("NEO4J_URI", "bolt+ssc://localhost:7687")

NEO4J_USERNAME = "neo4j"
NEO4J_PASSWORD = "password"
if os.environ.get("NEO4J_USERNAME"):
    NEO4J_USERNAME = os.environ.get("NEO4J_USERNAME", "DUMMY")
    NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "neo4j")

gds = GraphDataScience(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))
gds.set_database("neo4j")
'

# Reset the database/catalog after each snippet so tests stay independent.
# Written against the 2.0 typed API (no `beta` namespace; typed result objects; snake_case kwargs).
CLEAN_UP = '
finally:
    for graph_info in gds.graph.list():
        gds.graph.drop(graph_info.graph_name, fail_if_missing=False)
    for pipeline_entry in gds.pipeline.list():
        gds.pipeline.drop(pipeline_entry.pipeline_name, fail_if_missing=False)
    for model_details in gds.model.list():
        if model_details.stored:
            gds.model.delete(model_details.model_name)
        gds.model.drop(model_details.model_name, fail_if_missing=False)
    gds.run_cypher("MATCH (n) DETACH DELETE (n)")
'

# The doc tests target the plugin/self-managed deployment. Snippets nested inside an Aura Graph
# Analytics or AuraDS tab are skipped, since they use session/Aura-specific setup.
NON_PLUGIN_TAB_ROLES = %w[
  include-with-Aura-Graph-Analytics
  include-with-attached
  include-with-self-managed
  include-with-standalone
  include-with-AuraDS
].freeze

# A block is eligible when it is not nested inside a non-plugin deployment tab
# (i.e. it is an untabbed snippet or lives in the `include-with-Neo4j-server` tab).
def plugin_eligible?(block)
  node = block
  while node
    roles = node.respond_to?(:roles) ? node.roles : []
    return false if (roles & NON_PLUGIN_TAB_ROLES).any?

    node = node.parent
  end
  true
end

def doc_files
  files = Dir["#{__dir__}/../modules/ROOT/pages/**/*.adoc"]
  # Optional substring filter to iterate on a single page, e.g. DOC_TEST_FILE=pipelines
  filter = ENV.fetch('DOC_TEST_FILE', nil)
  filter ? files.select { |f| f.include?(filter) } : files
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
    raw_code = "if ServerVersion.from_string(\"#{min_gds_version}\") <= gds.server_version():\n"
    block.source.each_line { |line| raw_code += "    #{line}" }
    raw_code
  else
    block.source
  end
end

# A block is testable if it is a runnable python source block for the current deployment
# lane and is not opted out via the `no-test` role or the `session` attribute.
def testable?(block)
  !block.has_role?('no-test') &&
    block.attr('language') == 'python' &&
    plugin_eligible?(block) &&
    !block.attr?('session')
end

# networkx-tagged blocks are run exclusively in the :networkx scope, and excluded elsewhere.
def filter_by_networkx(blocks, scope)
  if scope == :networkx
    blocks.select { |b| b.attr? 'networkx' }
  else
    blocks.reject { |b| b.attr? 'networkx' }
  end
end

def filter_source_blocks(source_blocks, scope)
  blocks = source_blocks.select { |b| testable?(b) }
  blocks = blocks.reject { |b| b.attr? 'enterprise' } unless scope == :enterprise
  filter_by_networkx(blocks, scope)
end

def scripts_of_file(path, scope)
  doc = Asciidoctor.load_file path, safe: :safe

  source_blocks = doc.find_by style: 'source'
  testable_source_blocks = filter_source_blocks(source_blocks, scope)
  skipped = source_blocks.count { |b| b.attr('language') == 'python' && b.has_role?('no-test') }

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

  [complete_raw_scripts(raw_scripts), skipped]
end

class DocTest < Minitest::Test
  def run_doc_scripts(scope)
    failures = []

    all_files = doc_files.map { |f| [f, *scripts_of_file(f, scope)] }
    total_skipped = all_files.sum { |entry| entry[2] }

    # Only files that actually contain testable snippets, so the progress numbering is contiguous.
    testable = all_files.reject { |entry| entry[1].empty? }
    total = testable.sum { |entry| entry[1].size }

    log_fully_skipped_files(all_files)
    LOGGER.info(
      "Running doc tests (scope=#{scope}): #{total} script(s) across #{testable.size} file(s); " \
      "#{total_skipped} code cell(s) skipped"
    )

    testable.each_with_index { |entry, idx| run_file(entry, idx + 1, testable.size, failures) }

    LOGGER.info(
      "Executed #{total} script(s) across #{testable.size} file(s); " \
      "#{failures.size} failed; #{total_skipped} code cell(s) skipped"
    )

    # Report every broken snippet at once rather than stopping at the first, so a
    # contributor sees the full list of docs to fix in a single run.
    assert failures.empty?, "#{failures.size} doc test(s) failed:\n\n#{failures.join("\n\n#{'-' * 80}\n\n")}"
  end


  def log_fully_skipped_files(all_files)
    all_files.each do |path, scripts, skipped|
      next unless scripts.empty? && skipped.positive?

      LOGGER.info("#{File.basename(path)}: 0 script(s), #{skipped} code cell(s) skipped")
    end
  end

  def run_file(entry, position, total_files, failures)
    path, scripts, skipped = entry
    name = File.basename(path)
    LOGGER.info(
      "[#{position}/#{total_files}] #{name}: running #{scripts.size} script(s), #{skipped} code cell(s) skipped"
    )
    before = failures.size

    scripts.each_with_index { |s, i| run_script(path, s, i + 1, scripts.size, failures) }

    failed = failures.size - before
    if failed.zero?
      LOGGER.info("  #{name}: all #{scripts.size} script(s) passed")
    else
      LOGGER.error("  #{name}: #{failed}/#{scripts.size} script(s) failed")
    end
  end

  def run_script(path, script, position, total, failures)
    started = Time.now
    # Feed the script via stdin rather than `python -c '...'`: the `-c` wrapper
    # corrupts any snippet containing single quotes.
    stdout, stderr, status = Open3.capture3(ARGV[0], stdin_data: script)
    elapsed = Time.now - started

    if status.success?
      LOGGER.debug("  script #{position}/#{total} passed (#{elapsed.round(1)}s)")
    else
      LOGGER.error("  script #{position}/#{total} failed (#{elapsed.round(1)}s)")
      failures << "A doc test of file '#{path}' failed:\n\nTest script: #{script}\nstdout: #{stdout}\nstderr: #{stderr}"
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
