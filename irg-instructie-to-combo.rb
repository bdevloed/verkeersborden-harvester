require 'linkeddata'
require 'csv'
require 'digest'

class InstructieHarvester
  ORG = RDF::Vocab::ORG
  FOAF = RDF::Vocab::FOAF
  SKOS = RDF::Vocab::SKOS
  DC = RDF::Vocab::DC
  PROV = RDF::Vocab::PROV
  RDFS = RDF::Vocab::RDFS
  REGORG = RDF::Vocabulary.new('https://www.w3.org/ns/regorg#')
  MU = RDF::Vocabulary.new('http://mu.semte.ch/vocabularies/core/')
  NFO = RDF::Vocabulary.new('http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#')
  NIE = RDF::Vocabulary.new('http://www.semanticdesktop.org/ontologies/2007/01/19/nie#')
  DBPEDIA = RDF::Vocabulary.new('http://dbpedia.org/ontology/')
  ADMS = RDF::Vocabulary.new('http://www.w3.org/ns/adms#')
  SV = RDF::Vocabulary.new('http://www.w3.org/2003/06/sw-vocab-status/ns#')

  MOB = RDF::Vocabulary.new('https://data.vlaanderen.be/ns/mobiliteit#')
  MANDAAT = RDF::Vocabulary.new('http://data.vlaanderen.be/ns/mandaat#')
  BESLUIT = RDF::Vocabulary.new('http://data.vlaanderen.be/ns/besluit#')
  EXT = RDF::Vocabulary.new('http://mu.semte.ch/vocabularies/ext/')
  LBLOD_MOW = RDF::Vocabulary.new('http://data.lblod.info/vocabularies/mobiliteit/')

  def initialize(input_verkeersborden, input_instructie)
    @repo = RDF::Graph.load(input_verkeersborden)
    @csv_path = input_instructie
    @output = 'output/verkeersborden-combinaties.ttl'
  end

  def harvest
    index = 1
    begin
      RDF::Graph.new do |graph|
        data = CSV.read(@csv_path, { headers: :first_row, encoding: 'utf-8', quote_char: '"' })
        data['maatregel_naam'].uniq.each do |measure_name|
          statements = road_sign_combinations(measure_name)
          if statements.length.positive?
            graph.insert_statements(statements)
          end
          index += 1
        end
        File.write(@output, graph.dump(:ttl), mode: 'w')
      end
    rescue Exception => e
      puts "error on line #{index}"
      raise e
    end
  end

  def find_verkeersbord(verkeersbord_code)
    return nil if verkeersbord_code.nil?

    if index = verkeersbord_code.index('Type')
      extract = verkeersbord_code.slice(index + 5, verkeersbord_code.length)
      puts "mapped #{verkeersbord_code} to G#{extract}"
      verkeersbord_code = "G#{extract}"
    end

    if verkeersbord_code.match /^X[a-z]$/
      mapped = "G#{verkeersbord_code}"
      puts "mapped #{verkeersbord_code} to #{mapped}"
      verkeersbord_code = mapped
    end
    query = RDF::Query.new({
                             bord: {
                               RDF.type => MOB['Verkeersbordconcept'],
                               SKOS.prefLabel => verkeersbord_code
                             }
                           })
    result = query.execute(@repo)
    result.first[:bord] if result.length === 1
  end

  def parse_measure(measure_name)
    combinations = measure_name.to_s.split('/', -1)
    combinations_with_subsigns = []
    combinations.each do |combination|
      combinations_with_subsigns << combination.split(/[-+]/, -1)
    end
    combinations_with_subsigns
  end

  def road_sign_combinations(measure_name)
    statements = []
    combinations_with_subsigns = parse_measure(measure_name)
    if combinations_with_subsigns.length.positive?
      main_sign_combinations = []
      combinations_with_subsigns.each do |combination|
        if combination.length.positive?
          main_sign_iri = find_verkeersbord(combination[0])
          if main_sign_iri
            main_sign_combinations << main_sign_iri
            subsigns = combination[1..-1]
            subsigns.each do |subsign|
              sub_sign_iri = find_verkeersbord(subsign)
              if sub_sign_iri
                statements << RDF::Statement.new(main_sign_iri, LBLOD_MOW['heeftOnderbordConcept'], sub_sign_iri)
              else
                puts "no road sign found for subsign with code #{subsign}"
              end
            end
          else
            puts "no road sign found for code #{combination[0]}"
          end
        else
          puts "#{combination} has no main sign"
        end
      end
      main_sign_combinations.permutation(2).to_a.each do |main_sign_combination|
        statements << RDF::Statement.new(main_sign_combination[0], LBLOD_MOW['heeftGerelateerdVerkeersbordconcept'], main_sign_combination[1])
      end
    else
      puts "#{measure_name} has no road sign combination"
    end
    statements
  end
end

harvester = InstructieHarvester.new('./output/verkeersborden.ttl', './input/verkeersmaatregel-templates.csv')
harvester.harvest
