# DRAFT CODE - not functioning. published as part of a handover

require 'linkeddata'
require 'csv'
require 'digest'
require 'set'
require 'rubygems'
require 'sparql'

# harvest sign/subsign relations and combinations from input html
# pun intended
class CombineHarvester
  DC = RDF::Vocab::DC
  FOAF = RDF::Vocab::FOAF
  ORG = RDF::Vocab::ORG
  PROV = RDF::Vocab::PROV
  RDFS = RDF::Vocab::RDFS
  SKOS = RDF::Vocab::SKOS
  SV = RDF::Vocab::VS
  ADMS = RDF::Vocabulary.new('http://www.w3.org/ns/adms#')
  DBPEDIA = RDF::Vocabulary.new('http://dbpedia.org/ontology/')
  MU = RDF::Vocabulary.new('http://mu.semte.ch/vocabularies/core/')
  NFO = RDF::Vocabulary.new('http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#')
  NIE = RDF::Vocabulary.new('http://www.semanticdesktop.org/ontologies/2007/01/19/nie#')
  REGORG = RDF::Vocabulary.new('https://www.w3.org/ns/regorg#')

  BESLUIT = RDF::Vocabulary.new('http://data.vlaanderen.be/ns/besluit#')
  EXT = RDF::Vocabulary.new('http://mu.semte.ch/vocabularies/ext/')
  LBLOD_MOW = RDF::Vocabulary.new('http://data.lblod.info/vocabularies/mobiliteit/')
  MANDAAT = RDF::Vocabulary.new('http://data.vlaanderen.be/ns/mandaat#')
  MOB = RDF::Vocabulary.new('https://data.vlaanderen.be/ns/mobiliteit#')

  def initialize(input_verkeersborden, file_name, output_folder)
    @repo = RDF::Graph.load(input_verkeersborden)
    @graph = RDF::Graph.new
    @input = File.join(input_folder, file_name)
    @input_folder = input_folder
    @output_folder = output_folder
    @code_list_output_folder = File.join(output_folder, 'codelists')
    @output_files = File.join(output_folder, 'files')
    FileUtils.mkdir_p @code_list_output_folder
    FileUtils.mkdir_p @output_files
  end

  def harvest
    data = load_table
    insert_combinations(data)
    write_graph_to_ttl(@output_folder, 'verkeersborden', @graph)
    write_graph_to_ttl(@code_list_output_folder, 'verkeersborden_concept_schemes', @graph_code_list)
  end

  # TODO: factor out (duplicate code)
  def find_verkeersbord(verkeersbord_code)
    return nil if verkeersbord_code.nil?

    queryable = RDF::Repository.load('etc/doap.ttl')
    sse = SPARQL.parse(%(
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        PREFIX mob: <https://data.vlaanderen.be/ns/mobiliteit#>
        SELECT ?bord WHERE {
            ?bord
                a mob:Verkeersbordconcept;
                skos:prefLabel ?verkeersbord_code
        } 
    ))
    queryable.query(sse) do |result|
      result.inspect
    end

    query = RDF::Query.new({
                             bord: {
                               RDF.type => MOB['Verkeersbordconcept'],
                               SKOS.prefLabel => verkeersbord_code
                             }
                           })

                           query.
    result = query.execute(@repo)
    @codes_without_road_sign_concept.add(verkeersbord_code) if result.length != 1
    result.first[:bord] if result.length === 1
  end
end

harvester = CombineHarvester.new('./output/verkeersborden.ttl', 'LijstVerkeersSignalisatie.html', './output')
harvester.harvest
