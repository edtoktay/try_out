from tag_pair_interpolate import MasterErrorTagsInterpolation

i = MasterErrorTagsInterpolation(
    batch_id=1,
    master_tag_id=1,
    error_tag_id=2)
print(i.process_tags())