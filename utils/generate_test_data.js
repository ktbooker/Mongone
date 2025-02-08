const faker = require('faker');
const fs = require('fs');

let data = [];
for (let i = 0; i < 10000; i++) {
  data.push({
    name: faker.name.findName(),
    email: faker.internet.email(),
    age: faker.datatype.number({ min: 18, max: 80 }),
    signupDate: faker.date.past().toISOString(),
    purchases: Array.from({ length: faker.datatype.number({ min: 1, max: 5 }) }).map(() => ({
      item: faker.commerce.productName(),
      price: faker.commerce.price(),
    }))
  });
}

fs.writeFileSync('largeDataset.json', JSON.stringify(data, null, 2));
console.log('Dataset generated!');