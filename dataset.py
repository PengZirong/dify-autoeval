def upload_data_to_langfuse(langfuse, ds_name_in_langfuse, data):

    langfuse.create_dataset_item(
        dataset_name=ds_name_in_langfuse,
        input=data["input"],
        expected_output=data["output"]
    )
    