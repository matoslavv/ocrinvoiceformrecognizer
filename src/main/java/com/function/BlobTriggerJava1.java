package com.function;

import com.azure.ai.formrecognizer.documentanalysis.DocumentAnalysisAsyncClient;
import com.azure.core.util.BinaryData;
import com.azure.core.util.polling.AsyncPollResponse;
import com.azure.storage.blob.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.microsoft.azure.functions.annotation.*;
import java.io.FileWriter;
import java.io.IOException;
import com.microsoft.azure.functions.ExecutionContext;
import java.util.*;
import com.azure.ai.formrecognizer.documentanalysis.DocumentAnalysisClientBuilder;
import com.azure.ai.formrecognizer.documentanalysis.models.*;
import com.azure.core.credential.AzureKeyCredential;
import reactor.core.publisher.Mono;

public class BlobTriggerJava1 {
	private static final String endpoint = "https://westeurope.api.cognitive.microsoft.com/";
	private static final String key = "37d19429102b45bd9890b5ef3ea0f083"; // Form Recognizer keys and endpoints
	private static final String modelId = "prebuilt-invoice";
	private final DocumentAnalysisAsyncClient client;

	private final BlobContainerAsyncClient blobContainerClient;
	private final String storageAccountEndpoint = "https://clazublb02.blob.core.windows.net";
	private final String containerName = "imtestjson"; // meno kontajnera, kam ukladat jsony
	private final String sasToken = "?sv=2021-12-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2023-03-08T21:44:30Z&st=2023-03-08T13:44:30Z&spr=https&sig=ElLKs1a0dv%2FWK08WJVDVWXUc30OeseC2LaYCt7Q%2B9U8%3D"; // Storage account - shared access signature

	private final ObjectMapper mapper;

	public BlobTriggerJava1() {
		this.client = new DocumentAnalysisClientBuilder()
				.credential(new AzureKeyCredential(key))
				.endpoint(endpoint)
				.buildAsyncClient();

		this.blobContainerClient =  new BlobServiceClientBuilder()
									.endpoint(storageAccountEndpoint)
									.sasToken(sasToken)
									.buildAsyncClient()
									.getBlobContainerAsyncClient(containerName);

		this.mapper = new ObjectMapper();
	}

	private String generateFileName(String fileName) {
		return new StringBuilder().append(fileName, 0, fileName.lastIndexOf('.')).append(".json").toString();
	}

	private void createFile(String fileName, String content) throws IOException {
		try (FileWriter file = new FileWriter(fileName)) {
			file.write(content);
		}
	}

	private String getInvoiceData(Map<String, DocumentField> invoiceData) throws JsonProcessingException {
		ObjectNode invoiceJSON = mapper.createObjectNode();

		DocumentField invoiceIdField = invoiceData.get("InvoiceId"); // poradove cislo faktury
		if (invoiceIdField != null && DocumentFieldType.STRING == invoiceIdField.getType()) {
			invoiceJSON.put("InvoiceId", invoiceIdField.getContent());
		}

		DocumentField vendorNameField = invoiceData.get("VendorAddressRecipient"); // cele meno predajcu
//		invoiceJSON.put("VendorName", vendorNameField.getContent() == null ? "" : vendorNameField.getContent());
		if (vendorNameField != null && DocumentFieldType.STRING == vendorNameField.getType()) {
			invoiceJSON.put("VendorName", vendorNameField.getContent());
		}

		DocumentField vendorAddressField = invoiceData.get("VendorAddress");
//		invoiceJSON.put("VendorAddress", vendorAddressField.getContent() == null ? "" : vendorAddressField.getContent());
		if (vendorAddressField != null && DocumentFieldType.STRING == vendorAddressField.getType()) {
			invoiceJSON.put("VendorAddress", vendorAddressField.getContent());
		}

		DocumentField customerNameField = invoiceData.get("BillingAddressRecipient"); // meno prijemcu - komu pride faktura na zaplatenie
//		invoiceJSON.put("BillingAddressRecipient", customerNameField.getContent() == null ? "" : customerNameField.getContent());
		if (customerNameField != null && DocumentFieldType.STRING == customerNameField.getType()) {
			invoiceJSON.put("BillingAddressRecipient", customerNameField.getContent());
		}

		DocumentField customerAddressRecipientField = invoiceData.get("BillingAddress"); // adresa prijemcu, kam pride faktura na zaplatenie
//		invoiceJSON.put("BillingAddress", customerAddressRecipientField.getContent() == null ? "" : customerAddressRecipientField.getContent());
		if (customerAddressRecipientField != null && DocumentFieldType.STRING == customerAddressRecipientField.getType()) {
			invoiceJSON.put("BillingAddress", customerAddressRecipientField.getContent());
		}

		DocumentField invoiceDateField = invoiceData.get("InvoiceDate"); // datum vystavenia faktury
//		invoiceJSON.put("InvoiceDate", invoiceDateField.getContent() == null ? "" : invoiceDateField.getContent());
		if (invoiceDateField != null && DocumentFieldType.DATE == invoiceDateField.getType()) {
			invoiceJSON.put("InvoiceDate", invoiceDateField.getContent());
		}

		DocumentField dueDateField = invoiceData.get("DueDate"); // datum splatnosti faktury
//		invoiceJSON.put("DueDate", dueDateField.getContent() == null ? "" : dueDateField.getContent());
		if (dueDateField != null && DocumentFieldType.DATE == dueDateField.getType()) {
			invoiceJSON.put("DueDate", dueDateField.getContent());
		}

		DocumentField customerTaxIdField = invoiceData.get("CustomerTaxId"); // DIC zakaznika sluziace na dane
//		invoiceJSON.put("CustomerTaxId", customerTaxIdField.getContent() == null ? "" : customerTaxIdField.getContent());
		if (customerTaxIdField != null && DocumentFieldType.STRING == customerTaxIdField.getType()) {
			invoiceJSON.put("CustomerTaxId", customerTaxIdField.getContent());
		}

		DocumentField vendorTaxIdField = invoiceData.get("VendorTaxId"); // DIC predajcu sluziace na dane
//		invoiceJSON.put("VendorTaxId", vendorTaxIdField.getContent() == null ? "" : vendorTaxIdField.getContent());
		if (vendorTaxIdField != null && DocumentFieldType.STRING == vendorTaxIdField.getType()) {
			invoiceJSON.put("VendorTaxId", vendorTaxIdField.getContent());
		}

		DocumentField subtotalField = invoiceData.get("SubTotal"); // informácia o základe z ceny
//		invoiceJSON.put("SubTotal", subtotalField.getContent() == null ? "" : subtotalField.getContent());
		if (subtotalField != null && DocumentFieldType.DOUBLE == subtotalField.getType()) {
			invoiceJSON.put("SubTotal", subtotalField.getContent());
		}

		DocumentField totalTaxField = invoiceData.get("TotalTax"); // informácia o sadzbe z dane
//		invoiceJSON.put("TotalTax", totalTaxField.getContent() == null ? "" : totalTaxField.getContent());
		if (totalTaxField != null && DocumentFieldType.DOUBLE == totalTaxField.getType()) {
			invoiceJSON.put("TotalTax", totalTaxField.getContent());
		}

		DocumentField paymentDetailsWrapperField = invoiceData.get("PaymentDetails"); // IBAN
		if (paymentDetailsWrapperField != null && paymentDetailsWrapperField.getType() == DocumentFieldType.LIST && !paymentDetailsWrapperField.getValueAsList().isEmpty()) {
			DocumentField paymentDetailsDataField = paymentDetailsWrapperField.getValueAsList().get(0);
			if (paymentDetailsDataField.getType() == DocumentFieldType.MAP) {
				HashMap<String, DocumentField> paymentDetails = (HashMap<String, DocumentField>) paymentDetailsDataField.getValueAsMap();
				DocumentField IBANField = paymentDetails.get("IBAN");
				invoiceJSON.put("IBAN", IBANField.getContent() == null ? "" : IBANField.getContent());
				if (IBANField != null && IBANField.getType() == DocumentFieldType.STRING) {
					invoiceJSON.put("IBAN", IBANField.getContent());
				}
			}
		}
//		else {
//			invoiceJSON.put("IBAN", "");
//		}

		DocumentField invoiceTotalField = invoiceData.get("InvoiceTotal"); // celkova suma s danou
//		invoiceJSON.put("InvoiceTotal", invoiceTotalField.getContent() == null ? "" : invoiceTotalField.getContent());
		if (invoiceTotalField != null && DocumentFieldType.DOUBLE == invoiceTotalField.getType()) {
			invoiceJSON.put("InvoiceTotal", invoiceTotalField.getContent());
		}

		DocumentField amountDueField = invoiceData.get("AmountDue"); // celkova cena na uhradu - je to vlastne invoiceTotal + previousUnpaidBill. Ak nemame previousUnpaidBill, tak dame 0 a amountTotal == invoiceTotal
//		invoiceJSON.put("AmountDue", amountDueField.getContent() == null ? "" : amountDueField.getContent());
		if (amountDueField != null && DocumentFieldType.DOUBLE == amountDueField.getType()) {
			invoiceJSON.put("AmountDue", amountDueField.getContent());
		}

		DocumentField invoiceItemsField = invoiceData.get("Items");
		if (invoiceItemsField != null && DocumentFieldType.LIST == invoiceItemsField.getType()) {

			List<ObjectNode> items = new ArrayList<>();
			List<DocumentField> invoiceItems = invoiceItemsField.getValueAsList();

			for (DocumentField invoiceItem : invoiceItems) {
				ObjectNode item = mapper.createObjectNode();
				Map<String, DocumentField> documentFieldMap = invoiceItem.getValueAsMap();

				for (Map.Entry<String, DocumentField> entry : documentFieldMap.entrySet()) {
					String key = entry.getKey();
					DocumentField documentField = entry.getValue();
					if ("Description".equals(key) && DocumentFieldType.STRING == documentField.getType()) { // aky produkt
						item.put("Description", documentField.getContent());
					}
					if ("Quantity".equals(key) && DocumentFieldType.DOUBLE == documentField.getType()) { // v akom mnozstve
						item.put("Quantity", documentField.getContent());
					}
				}
				items.add(item);
			}
			String jsonItems = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(items);
			invoiceJSON.put("Items", jsonItems);
		} else {
			invoiceJSON.put("Items", "[]");
		}

		return invoiceJSON.toString();
	}

	private Mono<Void> uploadFile(String fileName) {
		BlobAsyncClient blobClient = blobContainerClient.getBlobAsyncClient(fileName);
		return blobClient.uploadFromFile(fileName).then();
	}

	private void processInvoice(Map<String, DocumentField> invoiceFields, String fileName) {
		try {
			String invoiceData = getInvoiceData(invoiceFields);
			String fileNameJson = generateFileName(fileName);
			createFile(fileNameJson, invoiceData);
			uploadFile(fileNameJson).subscribe();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@FunctionName("BlobTriggerJava1")
	@StorageAccount("bakalarka0211_STORAGE")
	public void run(
			@BlobTrigger(
					name = "content",
					path = "imtest9/{name}",
					connection = "connect",
					dataType = "binary") byte[] content,
			@BindingName("name") String name,
			final ExecutionContext context) {
		context.getLogger().info("Blob trigger function: Name: " + name + " Size: " + content.length + " Bytes \n");

		BinaryData binaryData = BinaryData.fromBytes(content);
		client.beginAnalyzeDocument(modelId, binaryData)
			.flatMap(AsyncPollResponse::getFinalResult)
			.subscribe(analyzeResult -> {
				if (analyzeResult != null) {
					for(AnalyzedDocument document : analyzeResult.getDocuments()) {
						processInvoice(document.getFields(), name);
					}
				}
			});
	}
}
