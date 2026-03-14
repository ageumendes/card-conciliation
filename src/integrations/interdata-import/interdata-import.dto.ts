export class InterdataListSalesQueryDto {
  dateFrom?: string;
  dateTo?: string;
  page?: string;
  limit?: string;
  status?: string;
  acquirer?: string;
  search?: string;
  paymentType?: string;
  brand?: string;
  bucket?: string;
  sortBy?: string;
  sortDir?: string;
  verbose?: string;
}

export class InterdataApproveDto {
  id?: number;
  bucket?: string;
}
