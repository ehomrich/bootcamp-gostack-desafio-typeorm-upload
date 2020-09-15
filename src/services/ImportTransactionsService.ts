import fs from 'fs';
import csvParse from 'csv-parse';
import { getCustomRepository, getRepository, In } from 'typeorm';

import TransactionRepository from '../repositories/TransactionsRepository';
import Transaction from '../models/Transaction';
import Category from '../models/Category';

interface Row {
  title: string;
  value: number;
  type: 'income' | 'outcome';
  category: string;
}

class ImportTransactionsService {
  async execute(filePath: string): Promise<Transaction[]> {
    const transactionsRepository = getCustomRepository(TransactionRepository);
    const categoriesRepository = getRepository(Category);

    const readStream = await fs.createReadStream(filePath);
    const parser = csvParse({ from_line: 2 });
    const parsedCsv = readStream.pipe(parser);

    const transactions: Row[] = [];
    const categories: string[] = [];

    parsedCsv.on('data', async row => {
      const [title, type, value, category] = row.map((s: string) => s.trim());

      if (!title || !type || !value) return;

      transactions.push({ title, type, value, category });
      categories.push(category);
    });

    await new Promise(resolve => parsedCsv.on('end', resolve));

    const existingCategories = await categoriesRepository.find({
      where: { title: In(categories) },
    });
    const existingCategoryTitles = existingCategories.map(({ title }) => title);
    const categoriesToCreate = categories
      .filter(category => !existingCategoryTitles.includes(category))
      .filter((value, index, array) => array.indexOf(value) === index)
      .map(title => ({ title }));

    const newCategories = await categoriesRepository.create(categoriesToCreate);
    await categoriesRepository.save(newCategories);

    const allCategories = [...existingCategories, ...newCategories];

    const newTransactions = await transactionsRepository.create(
      transactions.map(({ title, value, type, category }) => ({
        title,
        value,
        type,
        category: allCategories.find(cat => cat.title === category),
      })),
    );
    await transactionsRepository.save(newTransactions);

    await fs.promises.unlink(filePath);

    return newTransactions;
  }
}

export default ImportTransactionsService;
