package com.projectai.projectai.mdrm;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RulesWorkbookParsingTest {

    @Test
    void shouldSkipPrimaryMdrmFromSecondaryMdrmColumnWhenParsingRuleWorkbook() throws IOException {
        RulesService rulesService = new RulesService(null, new MdrmProperties());
        byte[] workbookBytes;
        try (XSSFWorkbook workbook = new XSSFWorkbook(); ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            var sheet = workbook.createSheet("All_Edits");
            Row header = sheet.createRow(0);
            header.createCell(0).setCellValue("reporting_form");
            header.createCell(1).setCellValue("rule_number");
            header.createCell(2).setCellValue("primary_mdrm");
            header.createCell(3).setCellValue("rule_expression");
            header.createCell(4).setCellValue("secondary_mdrms");

            Row row = sheet.createRow(1);
            row.createCell(0).setCellValue("FR Y-9C");
            row.createCell(1).setCellValue("R-1");
            row.createCell(2).setCellValue("BHCK2170");
            row.createCell(3).setCellValue("BHCK2170 = BHCK1000");
            row.createCell(4).setCellValue("BHCK2170, BHCK1000");

            workbook.write(output);
            workbookBytes = output.toByteArray();
        }

        Object parsedWorkbook = ReflectionTestUtils.invokeMethod(rulesService, "parseWorkbook", workbookBytes);
        @SuppressWarnings("unchecked")
        List<Object> dependencies = (List<Object>) ReflectionTestUtils.invokeMethod(parsedWorkbook, "dependencies");

        assertEquals(1, dependencies.size());
        assertEquals("BHCK2170", ReflectionTestUtils.invokeMethod(dependencies.get(0), "primaryMdrmCode"));
        assertEquals("BHCK1000", ReflectionTestUtils.invokeMethod(dependencies.get(0), "secondaryMdrmCode"));
    }
}
