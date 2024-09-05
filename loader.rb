# require 'open-uri'

GITHUB_BASE_URL = "https://raw.githubusercontent.com/Shahid5245/custom_ruby_scripts/main/"

FILES_TO_LOAD = [
  "result_hash/result_hash_v6_script.rb",
  "result_hash/validation_result_hash.rb"
]


FILES_TO_LOAD.each do |filename|
  file_content = URI.open(GITHUB_BASE_URL + filename).read
  eval(file_content)
end



