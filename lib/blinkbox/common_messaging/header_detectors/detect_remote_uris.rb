class Blinkbox::CommonMessaging::HeaderDetectors
  def detect_remote_uris(original_headers)
    hash = @obj.to_hash.extend(ExtraHashMethods)
    deep_keys = hash.deep_key_select do |h|
      h["type"] == "remote"
    end
    if !deep_keys.empty?
      original_headers.merge!(
        "has_remote_uris" => true,
        "remote_uris" => deep_keys
      )
    end
    original_headers
  end

  register :detect_remote_uris
end

module ExtraHashMethods
  def deep_key_select(parent_key: "", &block)
    keys = []
    keys.push parent_key if block.call(self)
    self.each do |k, v|
      case v
      when Hash
        v.extend(ExtraHashMethods).deep_key_select(parent_key: k, &block).each do |hit|
          keys.push [parent_key, hit].join(".").sub(/^\./,"")
        end
      when Array
        v.each_with_index do |item, i|
          if item.is_a? Hash
            item.extend(ExtraHashMethods).deep_key_select(parent_key: "#{k}[#{i}]", &block).each do |hit|
              keys.push [parent_key, hit].join(".").sub(/^\./,"")
            end
          end
        end
      end
    end
    keys
  end
end