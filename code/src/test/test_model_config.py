from config import ModelConfig

print(ModelConfig.one_line_string_config())
print(ModelConfig.from_dict({'loss': 'MSE', 'epoch': 4, 'batch_size': 33}).one_line_string_config())
