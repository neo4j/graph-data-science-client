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
# NEO4J_ARROW_URI (optional) points the client at a specific GDS Arrow server; when unset,
# the client auto-discovers the Arrow endpoint from the server (see the `arrow` parameter
# of GraphDataScience).
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

NEO4J_ARROW = os.environ.get("NEO4J_ARROW_URI", True)

gds = GraphDataScience(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD), arrow=NEO4J_ARROW)
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

# Boilerplate prepended to every AGA doc snippet: connect an AuraGraphDataScience object to
# the local GDS session and its Neo4j database (both started via testcontainers from the same
# images as the integration tests; see scripts/ci/run_doc_tests_aga.py). The `sessions.get_or_create`
# flow of the docs needs the Aura API, so the harness connects to the session directly instead.
INIT_AGA = '
import os
from unittest import mock

import pandas

from graphdatascience.arrow_client.arrow_authentication import UsernamePasswordAuthentication
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience
from graphdatascience.session.session_lifecycle_manager import SessionLifecycleManager

SESSION_ARROW_URI = os.environ.get("GDS_SESSION_ARROW_URI", "localhost:8491")
SESSION_HOST, SESSION_PORT = SESSION_ARROW_URI.rsplit(":", 1)
# The in-network address the session advertises; see tests/integration/services.py.
ADVERTISED_URI = os.environ.get("GDS_SESSION_ADVERTISED_ADDRESS", SESSION_ARROW_URI)
ADVERTISED_HOST, ADVERTISED_PORT = ADVERTISED_URI.rsplit(":", 1)

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USERNAME = os.environ.get("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "password")

arrow_client = AuthenticatedArrowClient(
    (SESSION_HOST, int(SESSION_PORT)),
    auth=UsernamePasswordAuthentication(NEO4J_USERNAME, NEO4J_PASSWORD),
    encrypted=False,
    advertised_listen_address=(ADVERTISED_HOST, int(ADVERTISED_PORT)),
)
db_query_runner = Neo4jQueryRunner.create_for_db(NEO4J_URI, (NEO4J_USERNAME, NEO4J_PASSWORD))
# The local `neo4j-aura-database` docker image does not advertise an "aura" kernel
# version, so claim Aura hosting explicitly (as the real session client does) to keep
# Aura-only features such as topological link prediction testable.
db_query_runner.hosted_in_aura = True
gds = AuraGraphDataScience(
    arrow_client,
    db_query_runner,
    session_lifecycle_manager=mock.Mock(spec=SessionLifecycleManager),
)
gds.set_database("neo4j")
'

# Reset the session catalog and the database after each AGA snippet so tests stay
# independent. Same shape as the plugin cleanup; session model-catalog operations
# route through the (mock) GDS API of the local test stack.
CLEAN_UP_AGA = '
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

# The doc tests run per deployment option, each selecting the snippets of one deployment
# of the manual together with the boilerplate client it needs:
#
# - plugin_community: Neo4j with the GDS plugin, unlicensed (community-safe snippets)
# - plugin_enterprise: Neo4j with the GDS plugin, licensed (also the enterprise snippets)
# - aga: local GDS session (sessions are always licensed)
#
# A deployment maps to a lane: snippets nested inside a deployment tab only run in the
# matching lane, untabbed snippets are deployment-neutral and run in every lane, unless
# they carry the `session` (AGA-only) or `plugin` (plugin-only) attribute.
DEPLOYMENTS = {
  plugin_community: { lane: :plugin, enterprise: false },
  plugin_enterprise: { lane: :plugin, enterprise: true },
  aga: { lane: :aga, enterprise: true }
}.freeze

# networkx-tagged blocks require the NetworkX extra (`graphdatascience[networkx]`) and
# run in every deployment; opt out via DOC_TEST_NETWORKX=no, e.g. when running the
# harness against an interpreter without the extra installed.
NETWORKX = ENV.fetch('DOC_TEST_NETWORKX', 'yes') == 'yes'

NON_PLUGIN_TAB_ROLES = %w[
  include-with-Aura-Graph-Analytics
  include-with-attached
  include-with-self-managed
  include-with-standalone
  include-with-AuraDS
].freeze

# All deployment tab roles used in the manual.
DEPLOYMENT_TAB_ROLES = (%w[include-with-GDS-database-plugin] + NON_PLUGIN_TAB_ROLES).freeze

# The tab role marking Aura Graph Analytics (GDS session) snippets.
AGA_TAB_ROLES = %w[include-with-Aura-Graph-Analytics].freeze

def ancestor_roles(block)
  roles = []
  node = block
  while node
    roles += node.respond_to?(:roles) ? node.roles : []
    node = node.parent
  end
  roles
end

# A block is eligible for the plugin lane when it is not nested inside a non-plugin
# deployment tab (i.e. it is an untabbed snippet or lives in the
# `include-with-GDS-database-plugin` tab).
def plugin_eligible?(block)
  (ancestor_roles(block) & NON_PLUGIN_TAB_ROLES).empty?
end

# A block belongs to the AGA lane if it is nested inside an Aura Graph Analytics tab,
# or carries the `session` attribute (session-only content such as async execution).
def aga_marked?(block)
  block.attr?('session') || !(ancestor_roles(block) & AGA_TAB_ROLES).empty?
end

# A block is untabbed when none of its ancestors carries a deployment tab role, making
# it deployment-neutral (it runs in both lanes).
def untabbed?(block)
  (ancestor_roles(block) & DEPLOYMENT_TAB_ROLES).empty?
end

def doc_files
  files = Dir["#{__dir__}/../modules/ROOT/pages/**/*.adoc"]
  # Optional substring filter to iterate on a single page, e.g. DOC_TEST_FILE=pipelines
  filter = ENV.fetch('DOC_TEST_FILE', nil)
  filter ? files.select { |f| f.include?(filter) } : files
end

def complete_raw_scripts(raw_scripts, deployment)
  init = deployment == :aga ? INIT_AGA : INIT_GDS
  clean_up = deployment == :aga ? CLEAN_UP_AGA : CLEAN_UP
  raw_scripts.map do |s|
    indented_s = "try:\n"
    s.each_line do |line|
      indented_s += "    #{line}"
    end
    init + indented_s + clean_up
  end
end

def block_to_raw_code(block, deployment)
  # Sessions have no server_version to gate on; min-server-version applies to the
  # plugin deployments only.
  return block.source if deployment == :aga || !block.attr?('min-server-version')

  min_gds_version = block.attr('min-server-version')
  raw_code = "if ServerVersion.from_string(\"#{min_gds_version}\") <= gds.server_version():\n"
  block.source.each_line { |line| raw_code += "    #{line}" }
  raw_code
end

# A block is testable if it is a runnable python source block for the given deployment
# and is not opted out via the `no-test` role.
def testable?(block, deployment)
  return false if block.has_role?('no-test') || block.attr('language') != 'python'

  if DEPLOYMENTS[deployment][:lane] == :aga
    # AGA: AGA-marked snippets, plus untabbed (deployment-neutral) snippets; the
    # `plugin` attribute marks plugin-only snippets (mirroring `session`).
    !block.attr?('plugin') && (aga_marked?(block) || untabbed?(block))
  else
    # Plugin: excludes session-only snippets (via the `session` attribute).
    plugin_eligible?(block) && !block.attr?('session')
  end
end

def filter_source_blocks(source_blocks, deployment)
  blocks = source_blocks.select { |b| testable?(b, deployment) }
  blocks = blocks.reject { |b| b.attr? 'enterprise' } unless DEPLOYMENTS[deployment][:enterprise]
  return blocks if NETWORKX

  blocks.reject { |b| b.attr? 'networkx' }
end

# Collect the raw script of each block; blocks sharing a `group` attribute are
# concatenated into one script (in document order).
def raw_scripts_of_blocks(blocks, deployment)
  raw_scripts = []
  raw_scripts_by_group = Hash.new { |h, k| h[k] = "# #{k}" }

  blocks.each do |b|
    if b.attr? 'group'
      group = b.attr 'group'
      raw_scripts_by_group[group] += "\n#{block_to_raw_code(b, deployment)}"
    else
      raw_scripts.push(block_to_raw_code(b, deployment))
    end
  end

  raw_scripts_by_group.each_value { |s| raw_scripts.push(s) }
  raw_scripts
end

def scripts_of_file(path, deployment)
  doc = Asciidoctor.load_file path, safe: :safe

  source_blocks = doc.find_by style: 'source'
  testable_source_blocks = filter_source_blocks(source_blocks, deployment)
  skipped = source_blocks.count { |b| b.attr('language') == 'python' && b.has_role?('no-test') }

  [complete_raw_scripts(raw_scripts_of_blocks(testable_source_blocks, deployment), deployment), skipped]
end

class DocTest < Minitest::Test
  def run_doc_scripts(deployment)
    failures = []

    all_files = doc_files.map { |f| [f, *scripts_of_file(f, deployment)] }
    total_skipped = all_files.sum { |entry| entry[2] }

    # Only files that actually contain testable snippets, so the progress numbering is contiguous.
    testable = all_files.reject { |entry| entry[1].empty? }
    total = testable.sum { |entry| entry[1].size }

    log_fully_skipped_files(all_files)
    LOGGER.info(
      "Running doc tests (deployment=#{deployment}): #{total} script(s) across #{testable.size} file(s); " \
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

  def test_plugin_community
    run_doc_scripts(:plugin_community)
  end

  def test_plugin_enterprise
    run_doc_scripts(:plugin_enterprise)
  end

  # Runs the Aura Graph Analytics parts of the manual (AGA tabs, `session` snippets, and
  # untabbed snippets in AGA files) against a local GDS session started by
  # scripts/ci/run_doc_tests_aga.py; see doc/README.md for how to run it.
  def test_aga
    run_doc_scripts(:aga)
  end
end
