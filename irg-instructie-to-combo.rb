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
  REGORG = RDF::Vocabulary.new("https://www.w3.org/ns/regorg#")
  MU = RDF::Vocabulary.new("http://mu.semte.ch/vocabularies/core/")
  NFO = RDF::Vocabulary.new("http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#")
  NIE =  RDF::Vocabulary.new("http://www.semanticdesktop.org/ontologies/2007/01/19/nie#")
  DBPEDIA = RDF::Vocabulary.new("http://dbpedia.org/ontology/")
  ADMS = RDF::Vocabulary.new('http://www.w3.org/ns/adms#')
  SV = RDF::Vocabulary.new('http://www.w3.org/2003/06/sw-vocab-status/ns#')

  MOB = RDF::Vocabulary.new("https://data.vlaanderen.be/ns/mobiliteit#")
  MANDAAT = RDF::Vocabulary.new("http://data.vlaanderen.be/ns/mandaat#")
  BESLUIT = RDF::Vocabulary.new("http://data.vlaanderen.be/ns/besluit#")
  EXT = RDF::Vocabulary.new("http://mu.semte.ch/vocabularies/ext/")
  LBLOD_MOW = RDF::Vocabulary.new("http://data.lblod.info/vocabularies/mobiliteit/")

  def initialize(input_verkeersborden, input_instructie)
    @repo = RDF::Graph.load(input_verkeersborden)
    @csv_path = input_instructie
    @output = "output/verkeersborden-combinaties.ttl"
  end


  def harvest
    index = 1
    begin
      RDF::Graph.new do |graph|
        ::CSV.foreach(@csv_path, {  headers: :first_row, encoding: 'utf-8', quote_char: "\""  }) do |row|
          statements = parse_row(index, row)
          if statements.length > 0
            graph.insert_statements(statements)
          end
          index += 1
        end
        query = %(
SELECT ?combinatie (GROUP_CONCAT(?part) as ?parts)
WHERE {
      ?combinatie a <#{LBLOD_MOW.Verkeersbordcombinatie}>; <#{DC.hasPart}> ?part.
} GROUP BY ?combinatie HAVING (COUNT(?part) < 2)
)
        SPARQL.execute(query, graph).each_solution do |solution|
          puts "combinatie #{solution[:combinatie]} heeft maar 1 bord en wordt verwijderd"
          graph.delete([solution[:combinatie], nil, nil])
          graph.delete([RDF::URI(solution[:parts]), nil, nil])
        end
        File.write(@output, graph.dump(:ttl), mode: 'w')
      end
    rescue Exception => e
      puts "error on line #{index}"
      raise e
    end
  end

  def find_verkeersbord(verkeersbord_code)
    if verkeersbord_code.nil?
      return nil
    end
    if index = verkeersbord_code.index("Type")
      extract = verkeersbord_code.slice(index+5,verkeersbord_code.length)
      puts "mapped #{verkeersbord_code} to G#{extract}"
      verkeersbord_code= "G#{extract}"
    end
    query = RDF::Query.new({
                             bord: {
                               RDF.type  => MOB['Verkeersbordconcept'],
                               SKOS.prefLabel => verkeersbord_code
                             }
                           })
    result = query.execute(@repo)
    if result.length === 1
      return result.first[:bord]
    else
      return nil
    end

  end
  def parse_row(index, row)
    uuid = row.first[1]
    if uuid
      row_iri = RDF::URI("http://data.lblod.info/measure-concept/#{uuid}")
      verkeersbord_code = row["verkeersbord_code"]
      verkeersbord_iri = find_verkeersbord(verkeersbord_code)
      verkeersbord_instructie = row["instructie"]
      verkeersbord_aanvullende_instructie = row["aanvullende_instructie"]
      measure_name = row["maatregel_naam"]
      instruction_names = measure_name.to_s.split('/', -1)
      subInstructions = []
      instructions.each do |instruction|
        subInstructions << instruction.split(/[-+]/, -1)
      end
        
      statements = []
      if verkeersbord_iri
        instruction_uuid = Digest::MD5.hexdigest("#{uuid}#{verkeersbord_iri}")
        instruction_concept = RDF::URI("http://data.lblod.info/instruction-concept/#{instruction_uuid}")
        statements << RDF::Statement.new( row_iri, RDF.type, LBLOD_MOW["MeasureConcept"])
        statements << RDF::Statement.new( row_iri, LBLOD_MOW['hasInstruction'], instruction_concept )
        statements << RDF::Statement.new( row_iri, MU.uuid, RDF::Literal.new(uuid))
        statements << RDF::Statement.new( instruction_concept, RDF.type, LBLOD_MOW["Instruction"])
        statements << RDF::Statement.new( instruction_concept, MU.uuid, instruction_uuid)
        statements << RDF::Statement.new( instruction_concept, DC.description, RDF::Literal.new(verkeersbord_instructie))
        statements << RDF::Statement.new( instruction_concept, LBLOD_MOW['verkeersbordconcept'], verkeersbord_iri)

        sub_instruction_uuid = Digest::MD5.hexdigest("#{verkeersbord_aanvullende_instructie}")
        sub_instruction_concept = RDF::URI("http://data.lblod.info/instruction-concept/#{sub_instruction_uuid}")
        statements << RDF::Statement.new( instruction_concept, LBLOD_MOW['hasSubInstruction'], sub_instruction_concept )
        statements << RDF::Statement.new( sub_instruction_concept, RDF.type, LBLOD_MOW["Instruction"])
        statements << RDF::Statement.new( sub_instruction_concept, MU.uuid, sub_instruction_uuid)
        statements << RDF::Statement.new( sub_instruction_concept, DC.description, RDF::Literal.new(verkeersbord_aanvullende_instructie))
        # statements << RDF::Statement.new( sub_instruction_concept, LBLOD_MOW['verkeersbordconcept'], verkeersbord_iri)

      else
        puts "row #{index} no road sign found for code #{verkeersbord_code.inspect}"
      end
      statements
    else
      puts "row #{index} has no uuid"
      []
    end
  end
end

harvester = InstructieHarvester.new('./output/verkeersborden.ttl', './input/verkeersmaatregel-templates.csv')
harvester.harvest
